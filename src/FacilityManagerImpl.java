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

public class FacilityManagerImpl implements FacilityManager {

	private static final String VALID_COMMANDS = "VALID COMMANDS:\n"
		+ "upload <filepath> <filename> \t \t \t Upload a file to the distributed file system.\n"
		+ "mapreduce <classpath> <input-filename> \t Run the specified mapreduce.\n"
		+ "stop <classname> <input-filename> \t Stop the specified mapreduce.\n"
		+ "ps \t \t \t \t \t \t List all active mapreduces.\n"
		+ "ps -a \t \t \t \t \t \t List all active/stopped/completed mapreduces.\n"
		+ "exit \t \t \t \t \t \t Exit the system.";

	public static String clusterName;
	public static String REGISTRY_MASTER_KEY = "_MASTER";
	public static String REGISTRY_SLAVE_KEY = "_SLAVE";

	private int id;
	protected FS fs;
	private Config config;
	private FacilityManagerMaster master;
	private Registry masterRegistry;
	private Registry registry;

	// Constructor used by master.
	public FacilityManagerImpl(Config config) throws IOException {
		this.config = config;
		master = (FacilityManagerMaster) this;
		String[] participants = config.getParticipantIps();
		for (int i = 0; i < participants.length; i++) {
			if (participants[i].equals(config.getMasterIp())) {
				id = i;
				break;
			}
		}
		fs = new FS(this, master);
	}

	// Constructor used by a slave.
	public FacilityManagerImpl(String masterIp, int id, int port, String clusterName)
		throws UnknownHostException, IOException, NotBoundException, AlreadyBoundException {
		this.id = id;
		registry = LocateRegistry.createRegistry(port);
		registry.bind(clusterName + REGISTRY_SLAVE_KEY, UnicastRemoteObject.exportObject(this, 0));
		masterRegistry = LocateRegistry.getRegistry(masterIp, port);
		master = (FacilityManagerMaster) masterRegistry.lookup(clusterName + REGISTRY_MASTER_KEY);
		config = master.connect(id);
		fs = new FS(this, master);
	}

	public String runCommand(String command) throws RemoteException {
		if (command.startsWith("upload")) {
			uploadCmd(command);
		} else if (command.startsWith("mapreduce")) {
			mapreduceCmd(command);
		} else if (command.startsWith("stop")) {
			stopCmd(command);
		} else if (command.equals("ps")) {
			return master.getActiveProgramsList();
		} else if (command.equals("ps -a")) {
			return master.getActiveProgramsList().concat(master.getCompletedProgramsList());
		} else if (command.equals("exit")) {
			exit();
		} else {
			return VALID_COMMANDS;
		}
		return null;
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
				+ "mapreduce <classpath> <input-filename> ");
			return;
		}

		String classPath = params[1];
		String filename = params[2];
		File classfile = new File(classPath);
		if (!classfile.exists()) {
			System.out.println("The local java class file cannot be found.");
			return;
		}

		try {
			if (!master.hasDistributedFile(filename)) {
				System.out.println("The input file has not been uploaded into the distributed "
					+ "file system.");
				return;
			}
			URLClassLoader ucl = new URLClassLoader(new URL[] { new URL("file://" + fs.getRoot()) });
			Class<?> clazz = ucl.loadClass(classPath.substring(0, classPath.indexOf('.')));

			int jobId = dispatchJob(clazz, filename);
			if (jobId == -1) {
				System.out.println("There was a problem running the map reduce job.");
			} else {
				System.out.printf("The job was succesfully dispatched with id %s.%n", jobId);
			}
		} catch (MalformedURLException e) {
			System.out.println("There was an error loading the class file.");
		} catch (ClassNotFoundException e) {
			System.out.println("There was an error loading the class file.");
		} catch (RemoteException e) {
			System.out.println("There was an error communicating with the master.");
		}
	}

	public void stopCmd(String command) throws RemoteException {
		String[] params = command.split(" ");
		if (params.length != 3) {
			System.out.println("This command is of the form: "
				+ "stop <classname> <input-filename> ");
			return;
		}
		master.stopProgram(params[1], params[2]);
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
	public int dispatchJob(Class<?> clazz, String filename) throws RemoteException {
		fs.remoteWriteClass(clazz.getResourceAsStream(clazz.getName()), filename,
			config.getMasterIp());
		return master.dispatchJob(clazz, filename);
	}

	@Override
	public void runJob(NodeJob job) throws RemoteException {
		if (job instanceof MapJob) {
			runMapJob((MapJob) job);
		} else if (job instanceof MapCombineJob) {
			runMapCombineJob((MapCombineJob) job);
		} else if (job instanceof ReduceJob) {
			runReduceJob((ReduceJob) job);
		} else if (job instanceof ReduceCombineJob) {
			runReduceCombineJob((ReduceCombineJob) job);
		} else {
			System.out.println("Error");
		}
	}

	@Override
	public void runMapJob(MapJob mapJob) throws RemoteException {
		try {
			MapReduce440 mr = (MapReduce440) mapJob.getClazz().newInstance();
			Mapper440 mapper = mr.createMapper();

			mapper.setMaster(master);
			mapper.setFS(fs);
			mapper.setMapJob(mapJob);

			mapper.start();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void runMapCombineJob(MapCombineJob mcJob) throws RemoteException {
		MapCombiner440 combiner = new MapCombiner440();

		combiner.setMaster(master);
		combiner.setFs(fs);
		combiner.setMapCombineJob(mcJob);

		combiner.start();
	}

	@Override
	public void runReduceJob(ReduceJob reduceJob) throws RemoteException {
		MapReduce440 mr;
		try {
			mr = (MapReduce440) reduceJob.getClazz().newInstance();
			Reducer440 reducer = mr.createReducer();

			reducer.setMaster(master);
			reducer.setFS(fs);
			reducer.setReduceJob(reduceJob);

			reducer.start();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void runReduceCombineJob(ReduceCombineJob rcJob) throws RemoteException {
		// Gather and combine reduce files using ReduceCombiner440.
		ReduceCombiner440 combiner = new ReduceCombiner440();

		combiner.setMaster(master);
		combiner.setFs(fs);
		combiner.setReduceCombineJob(rcJob);

		combiner.start();
	}
}