import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
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
import java.util.Scanner;
import java.util.Set;

public class FacilityManagerMaster extends FacilityManagerLocal implements FacilityManager {

	private static String startParticipantScript = "start_participant.sh";

	private int rmiPort;
	private Map<Integer, FacilityManager> managers;
	private Map<String, Map<Integer, Set<Integer>>> fsTable;
	private int currentNode;

	private HealthChecker healthChecker;

	public FacilityManagerMaster(Config config) throws IOException, AlreadyBoundException,
		InterruptedException {
		super(config);
		currentNode = 1;
		int expectedNumParticipants = config.getParticipantIps().length;
		managers = Collections.synchronizedMap(new HashMap<Integer, FacilityManager>(
			expectedNumParticipants));
		managers.put(getNodeId(), this);
		rmiPort = config.getMrPort();

		/* FILESYSTEM INIT */
		fsTable = Collections.synchronizedMap(new HashMap<String, Map<Integer, Set<Integer>>>());

		Registry r = LocateRegistry.createRegistry(rmiPort);
		r.bind(REGISTRY_MASTER_KEY, UnicastRemoteObject.exportObject(this, 0));
		connectParticipants();

		healthChecker = new HealthChecker(this);

		System.out.println("Waiting for slaves to connect...");
		while (managers.size() != expectedNumParticipants) {
			Thread.sleep(1000);
		}
		healthChecker.start();

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
				+ rmiPort);
			pb.redirectErrorStream(true);
			pb.directory(null);
			Process p = pb.start();
			inheritIO(p.getInputStream(), System.out);
			inheritIO(p.getErrorStream(), System.err);
		}
	}

	private static void inheritIO(final InputStream src, final PrintStream dest) {
		new Thread(new Runnable() {
			public void run() {
				Scanner sc = new Scanner(src);
				while (sc.hasNextLine()) {
					dest.println(sc.nextLine());
				}
			}
		}).start();
	}

	@Override
	public synchronized Map<Integer, Set<Integer>> distributeBlocks(String namespace, int numRecords)
		throws RemoteException {
		int blockSize = getConfig().getBlockSize();
		int numBlocks = numRecords / blockSize + (numRecords % blockSize == 0 ? 0 : 1);
		Map<Integer, Set<Integer>> blocksToNodes = new HashMap<Integer, Set<Integer>>();
		for (int i = 0; i < numBlocks; i++) {
			for (int j = 0; j < getConfig().getReplicationFactor(); j++) {
				while (!healthChecker.isHealthy(currentNode)) {
					currentNode = (currentNode + 1) % (getConfig().getParticipantIps().length);
				}
				blocksToNodes.get(i).add(currentNode);
				blocksToNodes.put(i, blocksToNodes.get(i));
			}
		}

		return blocksToNodes;
	}

	@Override
	public Config connect(int id) throws RemoteException, NotBoundException {
		String slaveIp = getConfig().getParticipantIps()[id];
		System.out.println("Slave Connected: " + slaveIp);
		Registry registry = LocateRegistry.getRegistry(slaveIp, rmiPort);
		FacilityManager slaveManager = (FacilityManager) registry.lookup(REGISTRY_SLAVE_KEY);
		managers.put(id, slaveManager);
		healthChecker.addConnection(id, slaveManager);
		System.out.println(slaveManager.heartBeat());
		return getConfig();
	}

	@Override
	public synchronized int redistributeBlock(int nodeId) {
		if (nodeId == currentNode) {
			currentNode++;
		}
		return currentNode++;
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
		for (Integer id : managers.keySet()) {
			if (id == getNodeId()) {
				continue;
			}
			try {
				managers.get(id).exit();
			} catch (RemoteException e) {
				// Ignore, exception should be thrown
			}
		}
		System.out.println("Shutting down master...");
		System.exit(0);
	}

	public void slaveDied(int id) {
		managers.remove(id);
		System.out.println("Slave died " + getParticipantIp(id));
	}

	public String getParticipantIp(int id) {
		return getConfig().getParticipantIps()[id];
	}
}