import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FacilityManagerMasterImpl extends FacilityManagerImpl implements FacilityManagerMaster {

	private static String startParticipantScript = "start_participant.sh";

	private String clusterName;
	private int rmiPort;
	private FacilityManager[] managers;
	private Map<String, Map<Integer, Set<Integer>>> fsTable;
	private int currentNode;

	private HealthChecker healthChecker;
	private JobDispatcher dispatcher;
	private JobScheduler scheduler;

	public FacilityManagerMasterImpl(Config config) throws IOException, AlreadyBoundException,
		InterruptedException {
		super(config);

		currentNode = 0;
		String[] participants = config.getParticipantIps();
		int expectedNumParticipants = participants.length;

		managers = new FacilityManager[expectedNumParticipants];
		managers[getNodeId()] = this;

		clusterName = config.getClusterName();
		System.out.println("master cluster name = " + clusterName);
		rmiPort = config.getMrPort();

		/* FILESYSTEM INIT */
		fsTable = Collections.synchronizedMap(new HashMap<String, Map<Integer, Set<Integer>>>());

		Registry r = LocateRegistry.createRegistry(rmiPort);
		r.bind(clusterName + REGISTRY_MASTER_KEY, UnicastRemoteObject.exportObject(this, 0));
		connectParticipants();

		System.out.println("Waiting for slaves to connect...");
		while (managers.length != expectedNumParticipants) {
			Thread.sleep(1000);
		}
		
		healthChecker = new HealthChecker(this, expectedNumParticipants);
		scheduler = new JobScheduler(this, config);
		dispatcher = new JobDispatcher(this, config);
		dispatcher.setScheduler(scheduler);
		scheduler.setDispatcher(dispatcher);
		scheduler.setHealthChecker(healthChecker);

		healthChecker.start();
		dispatcher.start();

		System.out.println("All slaves connected.");
	}

	public void connectParticipants() throws IOException {
		String localAddress = getConfig().getMasterIp();
		String[] participantIps = getConfig().getParticipantIps();
		// the index of the slave's address in the config file represents its id
		for (int i = 0; i < participantIps.length; i++) {
			String slaveIp = participantIps[i];
			if (slaveIp.equals(localAddress)) {
				continue;
			}
			// Execute script.
			System.out.println("Executing script to start " + slaveIp);
			String command = "./" + startParticipantScript;
			ProcessBuilder pb = new ProcessBuilder(command, slaveIp, localAddress, "" + i, ""
				+ rmiPort, clusterName);
			pb.redirectErrorStream(true);
			pb.directory(null);
			Process p = pb.start();
			Utils.inheritIO(p.getInputStream(), System.out);
			Utils.inheritIO(p.getErrorStream(), System.err);
		}
	}

	@Override
	public synchronized Map<Integer, Set<Integer>> distributeBlocks(String namespace, int numBlocks)
		throws RemoteException {
		Map<Integer, Set<Integer>> blocksToNodes = new HashMap<Integer, Set<Integer>>();
		for (int i = 0; i < numBlocks; i++) {
			for (int j = 0; j < getConfig().getReplicationFactor(); j++) {
				while (!healthChecker.isHealthy(currentNode)) {
					incrementCurrentNode();
				}
				Set<Integer> nodes = blocksToNodes.get(i);
				if (nodes == null) {
					nodes = new HashSet<Integer>();
					blocksToNodes.put(i, nodes);
				}
				nodes.add(currentNode);
				incrementCurrentNode();
			}
		}

		return blocksToNodes;
	}

	public void incrementCurrentNode() {
		currentNode = (currentNode + 1) % (getConfig().getParticipantIps().length);
	}

	@Override
	public Config connect(int id) throws RemoteException, NotBoundException {
		String slaveIp = getConfig().getParticipantIps()[id];
		System.out.println("Slave Connected: " + slaveIp);
		Registry registry = LocateRegistry.getRegistry(slaveIp, rmiPort);
		FacilityManager slaveManager = (FacilityManager) registry.lookup(clusterName + REGISTRY_SLAVE_KEY);
		managers[id] = slaveManager;
		return getConfig();
	}

	@Override
	public synchronized int redistributeBlock(int nodeId) {
		if (nodeId == currentNode) {
			currentNode++;
		}
		incrementCurrentNode();
		return currentNode;
	}

	@Override
	public void updateFSTable(String namespace, int blockIndex, int nodeId) throws RemoteException {
		Map<Integer, Set<Integer>> blocksToNodes = fsTable.get(namespace);
		if (blocksToNodes == null) {
			blocksToNodes = Collections.synchronizedMap(new HashMap<Integer, Set<Integer>>());
			fsTable.put(namespace, blocksToNodes);
		}
		Set<Integer> nodes = blocksToNodes.get(blockIndex);
		if (nodes == null) {
			nodes = Collections.synchronizedSet(new HashSet<Integer>());
			blocksToNodes.put(blockIndex, nodes);
		}
		nodes.add(nodeId);
	}

	@Override
	public void exit() {
		for (int id = 0; id < managers.length; id++) {
			if (id == getNodeId()) {
				continue;
			}
			try {
				getManager(id).exit();
			} catch (RemoteException e) {
				// Ignore, exception should be thrown
			}
		}
		System.out.println("Shutting down master...");
		System.exit(0);
	}

	public void slaveDied(int id) {
		managers[id] = null;
	}

	public String getParticipantIp(int id) {
		return getConfig().getParticipantIps()[id];
	}

	@Override
	public int dispatchJob(Class<?> clazz, String filename) throws RemoteException {
		System.out.println("Dispatched job on master");
		Map<Integer, Set<Integer>> blockLocations = fsTable.get(filename);
		if (blockLocations == null) {
			return -1;
		}
		return scheduler.issueJob(clazz, filename, blockLocations.size());
	}

	@Override
	public boolean hasDistributedFile(String filename) throws RemoteException {
		return fsTable.containsKey(filename);
	}

	@Override
	public void jobFinished(boolean success, NodeJob job) throws RemoteException {
		if (!success) {
			System.out.println("Job finished unsuccessfully " + job);
			dispatcher.enqueue(job);
		} else {
			System.out.println("Job finished successfully " + job);
			scheduler.jobFinished(job);
		}
	}

	@Override
	public Map<Integer, Set<Integer>> getBlockLocations(String filename) throws RemoteException {
		return fsTable.get(filename);
	}

	public FacilityManager getManager(int nodeId) {
		return managers[nodeId];
	}

	public boolean isNodeHealthy(int nodeId) {
		return managers[nodeId] != null;
	}

	@Override
	public String getActiveJobsList() throws RemoteException {
		String list = "Job \t Status \t Mappers \t Reducers \n";
		list = list.concat(scheduler.getActiveProgramsList());
		return list;
	}

	@Override
	public String getCompletedJobsList() throws RemoteException {
		return scheduler.getCompletedProgramsList();
	}
}