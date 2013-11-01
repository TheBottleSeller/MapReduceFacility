import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FacilityManagerMaster extends FacilityManagerLocal implements
		FacilityManager {

	private static String startParticipantScript = "start_participant.sh";
	private List<String> participants;
	private Map<String, FacilityManager> slaveManagers;
	private Map<Integer, String> slaveAddresses;
	private Map<String, Map<Integer, Set<Integer>>> fsTable;
	private int currentNode;

	private HealthChecker healthChecker;
	
	public FacilityManagerMaster(Config config) throws IOException,
			AlreadyBoundException, InterruptedException {
		super(config);
		int expectedNumParticipants = config.getParticipantIps().length;
		participants = Collections.synchronizedList(new ArrayList<String>(
				expectedNumParticipants));
		participants.add(config.getMasterIp());
		currentNode = 1;
		fsTable = Collections
				.synchronizedMap(new HashMap<String, Map<Integer, Set<Integer>>>());
		healthChecker = new HealthChecker(this);
		healthChecker.start();
		slaveManagers = new HashMap<String, FacilityManager>();

		Registry r = LocateRegistry.createRegistry(config.getMrPort());
		r.bind(REGISTRY_MASTER_KEY, UnicastRemoteObject.exportObject(this, 0));
		slaveAddresses = Collections
				.synchronizedMap(new HashMap<Integer, String>(
						expectedNumParticipants));
		connectParticipants();

		System.out.println("Waiting for slaves to connect...");
		while (participants.size() != expectedNumParticipants) {
			Thread.sleep(1000);
		}
		System.out.println("All slaves connected.");
	}

	public Thread createHealthChecker() {
		return new Thread(new Runnable() {
			public void run() {

			}
		});
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
			String command = "./" + startParticipantScript;
			ProcessBuilder pb = new ProcessBuilder(command, slaveIp,
					localAddress, "" + i, "" + getConfig().getMrPort());
			pb.directory(null);
			pb.start();
			slaveAddresses.put(i, slaveIp);
		}
	}

	@Override
	public synchronized Map<Integer, Set<Integer>> distributeBlocks(
			String namespace, int numRecords) throws RemoteException {
		int blockSize = getConfig().getBlockSize();
		int numBlocks = numRecords / blockSize
				+ (numRecords % blockSize == 0 ? 0 : 1);
		Map<Integer, Set<Integer>> blocksToNodes = Collections
				.synchronizedMap(new HashMap<Integer, Set<Integer>>());
		for (int i = 0; i < numBlocks; i++) {
			for (int j = 0; j < getConfig().getReplicationFactor(); j++) {
				while (!healthChecker.isHealthy(currentNode)) {
					currentNode = (currentNode + 1)
							% (getConfig().getParticipantIps().length);
				}
				blocksToNodes.get(i).add(currentNode);
				blocksToNodes.put(i, blocksToNodes.get(i));
			}
		}

		fsTable.put(namespace, blocksToNodes);
		return blocksToNodes;
	}

	@Override
	public Config connect(int id) throws RemoteException, NotBoundException {
		String slaveIp = getConfig().getParticipantIps()[id];
		System.out.println("Slave Connected: " + slaveIp);
		Registry registry = LocateRegistry.getRegistry(slaveIp, getConfig()
				.getMrPort());
		FacilityManager slaveManager = (FacilityManager) registry
				.lookup(REGISTRY_SLAVE_KEY);
		slaveManagers.put(slaveIp, slaveManager);
		healthChecker.addConnection(id, slaveManager);
		participants.add(slaveIp);
		return getConfig();
	}
	
	public void slaveDied(int id) {
		System.out.println("Slave died " + slaveAddresses.get(id));
	}
}