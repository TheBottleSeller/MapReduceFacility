import java.io.File;
import java.io.FileNotFoundException;
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
	protected FS fs;
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
	public FacilityManagerImpl(String masterIp, int id, int port)
			throws UnknownHostException, IOException, NotBoundException,
			AlreadyBoundException {
		this.id = id;
		registry = LocateRegistry.createRegistry(port);
		registry.bind(REGISTRY_SLAVE_KEY,
				UnicastRemoteObject.exportObject(this, 0));
		masterRegistry = LocateRegistry.getRegistry(masterIp, port);
		master = (FacilityManagerMaster) masterRegistry
				.lookup(REGISTRY_MASTER_KEY);
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
			System.out
					.println("This command is of the form: upload <filename> <namespace>.");
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
				System.out
						.println("The file system already contains a file with the same "
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
				System.out
						.println("The input file has not been uploaded into the distributed "
								+ "file system.");
				return;
			}
			URLClassLoader ucl = new URLClassLoader(new URL[] { new URL(
					"file://" + fs.getRoot()) });
			Class<?> clazz = ucl.loadClass(classPath.substring(0,
					classPath.indexOf('.')));

			int jobId = dispatchJob(clazz, namespace);
			if (jobId == -1) {
				System.out
						.println("There was a problem running the map reduce job");
			} else {
				System.out
						.println("The job was succesfully dispatched with id "
								+ jobId);
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			System.out
					.println("There was an error communicating with the master.");
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
	public int dispatchJob(Class<?> clazz, String filename)
			throws RemoteException {
		fs.remoteWriteClass(clazz.getResourceAsStream(clazz.getName()),
				filename, config.getMasterIp());
		return master.dispatchJob(clazz, filename);
	}

	@Override
	public boolean runMapJob(int jobId, String filename, int blockIndex,
			Class<?> clazz) throws RemoteException {
		System.out.println("Running local map job");
		File block = fs.makeFileBlock(filename, blockIndex);
		if (!block.exists()) {
			return false;
		}
		boolean success = false;
		try {
			MapReduce440 mr = (MapReduce440) clazz.newInstance();
			File outputBlock = fs.makeMappedFileBlock(filename, blockIndex,
					jobId);
			Mapper440<?, ?, ?, ?> mapper = mr.createMapper();

			// set necessary parameters
			mapper.setMaster(master);
			mapper.setInBlock(block);
			mapper.setOutBlock(outputBlock);
			mapper.setJobId(jobId);
			mapper.setNodeId(getNodeId());
			mapper.setBlockIndex(blockIndex);

			// init mapper by opening up files
			mapper.init();

			// start mapper
			mapper.start();
			success = true;
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return success;
	}

	@Override
	public boolean runCombineJob(Set<Integer> blockIndices, String filename,
			int jobId, int maxKey, int minKey, int numReducers)
			throws RemoteException {

		boolean success = false;
		Combiner440 combiner = new Combiner440();

		combiner.setMaster(master);
		combiner.setFs(fs);
		combiner.setJobId(jobId);
		combiner.setNodeId(getNodeId());
		combiner.setBlockIndices(blockIndices);
		combiner.setFilename(filename);
		combiner.setMaxKey(maxKey);
		combiner.setMinKey(minKey);
		combiner.setNumReducers(numReducers);

		try {
			combiner.init();
			success = true;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return success;
	}

	@Override
	public boolean distributePartitions(int jobId, String filename,
			int[] reducers) throws RemoteException {
		boolean success = true;
		for (int partitionNo = 0; partitionNo < reducers.length; partitionNo++) {
			String partitionPath = fs.createPartitionDataFilePath(filename,
					jobId, partitionNo);
			String remoteFilename = String.format("%s-jobId-%d-part-%d-%d",
					filename, jobId, partitionNo, getNodeId());
			try {
				fs.sendFile(partitionPath, remoteFilename,
						config.getNodeAddress(reducers[partitionNo]));
			} catch (FileNotFoundException e) {
				success = false;
				e.printStackTrace();
			}
		}
		return success;
	}

	@Override
	public boolean runReduceJob(int jobId, String filename, int partitionNo,
			int numMappers, Class<?> clazz) throws RemoteException {
		boolean success = false;
		MapReduce440 mr;
		try {
			mr = (MapReduce440) clazz.newInstance();
			Reducer440<?, ?, ?, ?> reducer = mr.createReducer();
			
			
			success = true;
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return success;
	}

}