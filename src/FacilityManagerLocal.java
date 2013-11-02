import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
	protected static String LOG_FILE = "error.log";

	private int id;
	private FSImpl fs;
	private Config config;
	private FacilityManager master;
	private Registry masterRegistry;
	private Registry registry;

	// constructor used by master
	public FacilityManagerLocal(Config config) throws IOException {
		createExceptionHandler();
		this.config = config;
		id = -1;
		fs = new FSImpl(this);
	}

	// constructor used by slave
	public FacilityManagerLocal(String masterIp, int id, int port) throws UnknownHostException,
		IOException, NotBoundException, AlreadyBoundException {
		createExceptionHandler();
		this.id = id;
		masterRegistry = LocateRegistry.getRegistry(masterIp, port);
		master = (FacilityManager) masterRegistry.lookup(REGISTRY_MASTER_KEY);
		registry = LocateRegistry.createRegistry(port);
		registry.rebind(REGISTRY_SLAVE_KEY, UnicastRemoteObject.exportObject(this, 0));
		config = master.connect(id);
		fs = new FSImpl(this);
	}
	
	public void createExceptionHandler() {
		this.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread th, Throwable t) {
				File log = new File(LOG_FILE);
				if (!log.exists()) {
					try {
						log.createNewFile();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				try {
					PrintWriter writer = new PrintWriter(new FileWriter(log));
					t.printStackTrace(writer);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
	}

	public void run() {
		Scanner scanner = new Scanner(System.in);
		System.out.print(PROMPT);

		while (scanner.hasNextLine()) {
			String command = scanner.nextLine();
			System.out.println(PROMPT);
			/*
			 *  upload a file into the distributed file system
			 *  cmd: upload local-file-path namespace
			 */
			if (command.startsWith("upload")) {
				// Get the filename and namespace.
				int fileNameStart = command.indexOf(" ") + 1;
				int nameSpaceStart = command.lastIndexOf(" ") + 1;
				File file = new File(command.substring(fileNameStart, nameSpaceStart - 2));
				if (file.exists()) {
					String namespace = command.substring(nameSpaceStart);
					try {
						fs.upload(file, namespace);
					} catch (IOException e) {
						System.out.println("There was an error uploading the file!");
						e.printStackTrace();
					}
				} else {
					System.out.println("Error: File does not exist.");
					System.out.println(PROMPT);
				}
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
	public Map<Integer, Set<Integer>> distributeBlocks(String namespace,
			int numRecords) throws RemoteException {
		return master.distributeBlocks(namespace, numRecords);
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
	public void updateFSTable(String namespace, int blockIndex, int nodeId)
			throws RemoteException {
		master.updateFSTable(namespace, blockIndex, nodeId);
	}
}
