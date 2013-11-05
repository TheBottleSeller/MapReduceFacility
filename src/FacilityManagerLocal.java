import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class FacilityManagerLocal extends Thread implements FacilityManager {

	private static final String PROMPT = "=> ";
	protected static String REGISTRY_MASTER_KEY = "MASTER";
	protected static String REGISTRY_SLAVE_KEY = "SLAVE_HEALTH";

	private int id;
	private FSImpl fs;
	private Config config;
	private FacilityManager master;
	private Registry masterRegistry;
	private Registry registry;

	// Constructor used by master.
	public FacilityManagerLocal(Config config) throws IOException {
		this.config = config;
		fs = new FSImpl(this);
		master = this;
		String[] participants = config.getParticipantIps();
		for (int i = 0; i < participants.length; i++) {
			if (participants[i].equals(config.getMasterIp())) {
				id = i;
				break;
			}
		}
	}

	// Constructor used by a slave.
	public FacilityManagerLocal(String masterIp, int id, int port) throws UnknownHostException,
		IOException, NotBoundException, AlreadyBoundException {
		this.id = id;
		registry = LocateRegistry.createRegistry(port);
		registry.bind(REGISTRY_SLAVE_KEY, UnicastRemoteObject.exportObject(this, 0));
		masterRegistry = LocateRegistry.getRegistry(masterIp, port);
		master = (FacilityManager) masterRegistry.lookup(REGISTRY_MASTER_KEY);
		config = master.connect(id);
		fs = new FSImpl(this);
	}

	public void run() {
		Scanner scanner = new Scanner(System.in);
		System.out.print(PROMPT);

		while (scanner.hasNextLine()) {
			String command = scanner.nextLine();
			System.out.println(PROMPT);
			if (command.startsWith("upload") || command.startsWith("mapreduce")) {
				// Upload a file into the DFS OR perform mapreduce.
				int fileNameStart = command.indexOf(" ") + 1;
				int nameSpaceStart = command.lastIndexOf(" ") + 1;
				String fileName = command.substring(fileNameStart, nameSpaceStart - 1);
				File file = new File(fileName);
				if (file.exists()) {
					String namespace = command.substring(nameSpaceStart);
					try {
						if (command.startsWith("upload")) {
							fs.upload(file, namespace);
						} else {
							URLClassLoader ucl = new URLClassLoader(new URL[] { new URL("file://"
								+ fs.getRoot()) });
							Class<?> clazz = ucl.loadClass(fileName.substring(0,
								fileName.indexOf('.')));
							fs.remoteWriteClass(clazz.getResourceAsStream(fileName), namespace,
								master.getNodeId());
							master.loadClassObjects(clazz);
						}
					} catch (IOException e) {
						System.out.println("There was an error uploading the file!");
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				} else {
					System.out.println("Error: File does not exist.");
					System.out.println(PROMPT);
				}
			} else if (command.equals("exit")) {
				// Exit the system.
				exit();
			} else {
				System.out
					.println("Commands:\n"
						+ "upload <filename> <namespace>\tupload a file to the DFS.\n"
						+ "mapreduce <class-filename> <input-file-namespace>\trun the specified mapreduce.");
			}
		}
	}

	public Config getConfig() {
		return config;
	}

	@Override
	public Config connect(int id) throws RemoteException, NotBoundException {
		return null;
	}

	@Override
	public Map<Integer, Set<Integer>> distributeBlocks(String namespace, int numBlocks)
		throws RemoteException {
		return master.distributeBlocks(namespace, numBlocks);
	}

	@Override
	public boolean heartBeat() throws RemoteException {
		return true;
	}

	@Override
	public int getNodeId() {
		return id;
	}

	@Override
	public int redistributeBlock(int nodeId) throws RemoteException {
		return master.redistributeBlock(nodeId);
	}

	@Override
	public void updateFSTable(String namespace, int blockIndex, int nodeId) throws RemoteException {
		master.updateFSTable(namespace, blockIndex, nodeId);
	}

	public void exit() {
		System.exit(0);
	}

	@Override
	public void loadClassObjects(Class<?> clazz) throws RemoteException {
		// TODO Auto-generated method stub
	}
}
