import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;
import java.util.Set;

public class FacilityManagerImpl extends Thread implements FacilityManager {

	private static final String PROMPT = "=> ";
	protected static String REGISTRY_MASTER_KEY = "MASTER";
	protected static String REGISTRY_SLAVE_KEY = "SLAVE_HEALTH";

	private int id;
	private FS fs;
	private Config config;
	private FacilityManagerMaster master;
	private Registry masterRegistry;
	private Registry registry;

	// Constructor used by master.
	public FacilityManagerImpl(Config config) throws IOException {
		this.config = config;
		master = (FacilityManagerMaster) this;
		fs = new FS(this, master);
		String[] participants = config.getParticipantIps();
		for (int i = 0; i < participants.length; i++) {
			if (participants[i].equals(config.getMasterIp())) {
				id = i;
				break;
			}
		}
	}

	// Constructor used by a slave.
	public FacilityManagerImpl(String masterIp, int id, int port) throws UnknownHostException,
		IOException, NotBoundException, AlreadyBoundException {
		this.id = id;
		registry = LocateRegistry.createRegistry(port);
		registry.bind(REGISTRY_SLAVE_KEY, UnicastRemoteObject.exportObject(this, 0));
		masterRegistry = LocateRegistry.getRegistry(masterIp, port);
		master = (FacilityManagerMaster) masterRegistry.lookup(REGISTRY_MASTER_KEY);
		config = master.connect(id);
		fs = new FS(this, master);
	}

	public void run() {
		Scanner scanner = new Scanner(System.in);
		System.out.print(PROMPT);

		while (scanner.hasNextLine()) {
			String command = scanner.nextLine();
			System.out.println(PROMPT);
			if (command.startsWith("upload")) {
				uploadCmd(command);
			} else if (command.startsWith("mapreduce")) {
				mapreduceCmd(command);
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

	private void uploadCmd(String command) {
		String[] params = command.split(" ");
		if (params.length != 3) {
			System.out.println("This command is of the form: upload <filename> <namespace>.");
			return;
		}
		File file = new File(params[1]);
		String namespace = params[2];
		if (!file.exists()) {
			System.out.println("The local file cannot be found.");
			return;
		}

		try {
			if (master.hasDistributedFile(namespace)) {
				System.out.println("The file system already contains a file with the same "
					+ "namespace. Please choose another name");
				return;
			}

			fs.upload(file, namespace);
		} catch (IOException e) {
			System.out.println("There was an error while uploading the file.");
			e.printStackTrace();
		}
	}

	private void mapreduceCmd(String command) {
		String[] params = command.split(" ");
		if (params.length != 3) {
			System.out.println("This command is of the form: "
				+ "mapreduce <class-filename> <input-file-namespace> ");
			return;
		}

		String classPath = params[1];
		String namespace = params[2];
		File classfile = new File(classPath);
		if (!classfile.exists()) {
			System.out.println("The local java class file cannot be found.");
			return;
		}

		try {
			if (!master.hasDistributedFile(namespace)) {
				System.out.println("The input file has not been uploaded into the distributed "
					+ "file system.");
				return;
			}

			URLClassLoader ucl = new URLClassLoader(new URL[] { new URL("file://" + fs.getRoot()) });
			Class<?> clazz = ucl.loadClass(classPath.substring(0, classPath.indexOf('.')));
			fs.remoteWriteClass(clazz.getResourceAsStream(classPath), namespace,
				config.getMasterIp());
			Job job = master.dispatchJob(clazz, namespace);
			System.out.println("The job was succesfully dispatched:");
			System.out.println(job.toString());

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			System.out.println("There was an error communicating with the master.");
		}
	}

	public Config getConfig() {
		return config;
	}

	@Override
	public boolean heartBeat() throws RemoteException {
		return true;
	}

	@Override
	public int getNodeId() {
		return id;
	}

	public void exit() {
		System.exit(0);
	}

	@Override
	public boolean runMapJob(int jobId, String filename, int blockIndex, Class<?> clazz)
		throws RemoteException {
		File block = fs.getFileBlock(filename, blockIndex);
		if (!block.exists()) {
			return false;
		}
		try {
			MapReduce440 mr = (MapReduce440) clazz.newInstance();
			File outputBlock = fs.getTempFileBlock(filename, blockIndex, jobId);
			Mapper440<?, ?, ?, ?> mapper = mr.createMapper(master, block, outputBlock, jobId,
				getNodeId(), blockIndex);
			mapper.start();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean runCombineJob(Set<Integer> blockIndices, String namespace, int jobId)
		throws RemoteException {
		// TODO Auto-generated method stub
		return false;
	}
}
