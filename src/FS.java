import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FS {

	private static final String DATA_PATH = "/tmp/data440-neha/";
	private static final String CLASS_PATH = "class440/";

	private static int writePort;
	private static int readPort;

	private Map<String, Set<Integer>> localFiles;
	private Map<Integer, Set<File>> partitionFiles;
	private FacilityManager manager;
	private FacilityManagerMaster master;
	private int blockSize;
	private WriteServer writeServer;
	private ReadServer readServer;

	private enum Messages {
		WRITE_DATA, WRITE_CLASS, WRITE_PARTITION
	};

	public enum FileType {
		USER_DATA, MAPPED, PARTITION, REDUCER_IN, REDUCER_OUT, FINAL_OUTPUT
	};

	public FS(FacilityManager manager, FacilityManagerMaster master) throws IOException {
		localFiles = Collections.synchronizedMap(new HashMap<String, Set<Integer>>());
		partitionFiles = Collections.synchronizedMap(new HashMap<Integer, Set<File>>());
		this.manager = manager;
		this.master = master;

		Config config = manager.getConfig();
		blockSize = config.getBlockSize();
		readPort = config.getFsReadPort();
		writePort = config.getFsWritePort();

		writeServer = new WriteServer();
		writeServer.start();
		readServer = new ReadServer();
		readServer.start();

		// Set up root directories.
		createRootDirectory(DATA_PATH);
		createRootDirectory(getRoot() + CLASS_PATH);
	}

	public String getRoot() {
		return System.getProperty("user.home") + "/";
	}

	private void createRootDirectory(String path) {
		File root = new File(path);
		if (root.exists() || !root.isDirectory()) {
			root.delete();
		}
		root.mkdirs();
	}

	public void upload(File file, String namespace) throws IOException {
		int totalLines = getNumLines(file);
		int numBlocks = Math.max(1,
			(int) Math.ceil(totalLines * 1.0 / manager.getConfig().getBlockSize()));

		try {
			// get distribution of block index -> set of nodes to place block on
			Map<Integer, Set<Integer>> blockDistribution = master.distributeBlocks(namespace,
				numBlocks);

			BufferedReader reader = new BufferedReader(new FileReader(file));
			for (int i = 0; i < numBlocks; i++) {
				Set<Integer> nodes = blockDistribution.get(i);
				int numReplicated = 0;
				int numLines = Math.min(blockSize, totalLines - blockSize * i);

				for (int nodeId : nodes) {
					boolean success = false;
					int attempts = 0;
					int maxAttempts = manager.getConfig().getParticipantIps().length;
					while (attempts < maxAttempts) {
						// upper bound size of each line to 100 characters
						reader.mark(blockSize * 100);
						if (manager.getNodeId() == nodeId) {
							success = localWriteData(reader, namespace, i, numLines);
						} else {
							success = remoteWriteData(reader, nodeId, namespace, i, numLines);
						}
						if (numReplicated != nodes.size() - 1) {
							reader.reset();
						}
						if (!success) {
							nodeId = master.redistributeBlock(nodeId);
							attempts++;
						} else {
							master.updateFSTable(namespace, i, nodeId);
							break;
						}
					}
					if (success) {
						numReplicated++;
					} else {
						System.out.println("Could not upload file. No participants responding.");
					}
				}
			}
			reader.close();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public boolean localWriteData(BufferedReader reader, String namespace, int blockIndex,
		int numLines) {
		boolean success = false;
		File file = new File(createDataFilePath(namespace, blockIndex));
		if (file.exists()) {
			file.delete();
		}
		try {
			file.createNewFile();
			FileOutputStream fos = new FileOutputStream(file);
			PrintWriter writer = new PrintWriter(fos);
			for (int i = 0; i < numLines; i++) {
				String record = reader.readLine();
				writer.write(record);
				if (i != numLines - 1) {
					writer.write('\n');
				}
			}
			writer.close();
			addToLocalFiles(namespace, blockIndex);
			success = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return success;
	}

	public File getFile(String filename, int jobId, FileType type, int partNo, int fromNode) {
		String requestedFilename = createFilename(filename, jobId, type, partNo, fromNode);
		File localFile = new File(DATA_PATH + requestedFilename);
		try {
			if (fromNode == manager.getNodeId()) {
				return localFile;
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		if (localFile.exists()) {
			localFile.delete();
		}

		boolean success = false;
		try {
			localFile.createNewFile();
			String nodeAddress = manager.getConfig().getNodeAddress(fromNode);
			FileOutputStream fos = new FileOutputStream(localFile);

			Socket socket = new Socket(nodeAddress, readPort);

			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			out.flush();

			synchronized (this) {
				out.writeUTF(requestedFilename);
				out.flush();

				boolean hasFile = in.readBoolean();

				if (hasFile) {
					int numBytes = 0;
					while ((numBytes = in.readInt()) > 0) {
						byte[] bytes = new byte[numBytes];
						in.readFully(bytes, 0, numBytes);
						fos.write(bytes, 0, numBytes);
					}

					fos.flush();
					success = true;
				}
			}

			out.close();
			in.close();
			socket.close();
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (success) {
			return localFile;
		} else {
			return null;
		}
	}

	public boolean remoteWriteData(BufferedReader reader, int nodeId, String namespace,
		int blockIndex, int numLines) {
		String nodeAddress;
		boolean success = false;
		try {
			nodeAddress = manager.getConfig().getParticipantIps()[nodeId];
			Socket socket = new Socket(nodeAddress, writePort);

			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			out.flush();

			out.writeObject(Messages.WRITE_DATA);
			out.writeUTF(namespace);
			out.writeInt(blockIndex);
			out.writeInt(numLines);
			out.flush();

			for (int i = 0; i < numLines; i++) {
				out.writeUTF(reader.readLine());
				out.flush();
			}

			success = in.readBoolean();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return success;
	}

	public boolean remoteWriteClass(InputStream is, String namespace, String nodeAddress) {
		boolean success = false;
		try {
			Socket socket = new Socket(nodeAddress, writePort);

			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.flush();
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

			out.writeObject(Messages.WRITE_CLASS);
			out.writeUTF(namespace);

			try {
				byte[] c = new byte[1024];
				int numBytes = 0;
				while ((numBytes = is.read(c)) > 0) {
					out.writeInt(numBytes);
					out.write(c);
					out.flush();
				}
				out.writeInt(numBytes);
				out.flush();

				success = in.readBoolean();
			} finally {
				is.close();
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return success;
	}

	public void addToLocalFiles(String namespace, int blockIndex) {
		Set<Integer> blocks = localFiles.get(namespace);
		if (blocks != null) {
			blocks.add(blockIndex);
		} else {
			blocks = Collections.synchronizedSet(new HashSet<Integer>());
			blocks.add(blockIndex);
			localFiles.put(namespace, blocks);
		}
	}

	public File getFileBlock(String filename, int blockIndex) {
		return new File(createDataFilePath(filename, blockIndex));
	}

	public File makeMappedFileBlock(String filename, int blockIndex, int jobId) throws IOException {
		File temp = new File(createMappedDataFilePath(filename, jobId, blockIndex));
		if (temp.exists()) {
			temp.delete();
		}
		temp.createNewFile();
		return temp;
	}

	public File getMappedFileBlock(String filename, int blockIndex, int jobId) {
		try {
			return new File(createMappedDataFilePath(filename, jobId, blockIndex));
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return null;
	}

	public File makePartitionFileBlock(String filename, int jobId, int partitionNo)
		throws IOException {
		File partition = new File(createPartitionFilePath(filename, jobId, partitionNo));
		if (partition.exists()) {
			partition.delete();
		}
		partition.createNewFile();
		return partition;
	}

	public File getPartitionFileBlock(String filename, int jobId, int partitionNo)
		throws RemoteException {
		return new File(createPartitionFilePath(filename, jobId, partitionNo));
	}

	public File makeReduceInputFile(String filename, int jobId, int partitionNum)
		throws IOException {
		File partition = new File(createReducerInputFilePath(filename, jobId, partitionNum));
		if (partition.exists()) {
			partition.delete();
		}
		partition.createNewFile();
		return partition;
	}

	public File getReduceInputFile(String filename, int jobId, int partitionNum) {
		try {
			return new File(createReducerInputFilePath(filename, jobId, partitionNum));
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return null;
	}

	public File makeReduceOutputFile(String filename, int jobId, int partitionNum)
		throws IOException {
		File partition = new File(createReducerOutputFilePath(filename, jobId, partitionNum));
		if (partition.exists()) {
			partition.delete();
		}
		partition.createNewFile();
		return partition;
	}

	public File getReduceOutputFile(String filename, int jobId, int partitionNum) {
		try {
			return new File(createReducerOutputFilePath(filename, jobId, partitionNum));
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return null;
	}

	public File makeFinalOutputFile(String filename, int jobId) throws IOException {
		File output = new File(createFinalOutputFilePath(filename, jobId));
		if (output.exists()) {
			output.delete();
		}
		output.createNewFile();
		return output;
	}

	public File getFinalOutputFile(String filename, int jobId) {
		try {
			return new File(createFinalOutputFilePath(filename, jobId));
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return null;
	}

	private int getNumLines(File file) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(file));
		try {
			byte[] c = new byte[1024];
			int count = 0;
			int numBytes = 0;
			boolean empty = true;
			while ((numBytes = is.read(c)) != -1) {
				empty = false;
				for (int i = 0; i < numBytes; i++) {
					if (c[i] == '\n') {
						count++;
					}
				}
			}
			return (count == 0 && !empty) ? 1 : count;
		} finally {
			is.close();
		}
	}

	public class WriteServer extends Thread {

		private ServerSocket serverSocket;

		public WriteServer() throws IOException {
			serverSocket = new ServerSocket(writePort);
		}

		@Override
		public void run() {
			while (true) {
				try {
					Socket socket = serverSocket.accept();
					new WriterWorker(socket).start();
				} catch (IOException e) {
					// Ignore and keep listening.
				}
			}
		}

		public class WriterWorker extends Thread {
			private Socket socket;

			public WriterWorker(Socket socket) {
				this.socket = socket;
			}

			public void run() {
				ObjectOutputStream out = null;
				ObjectInputStream in = null;
				boolean success = false;
				try {
					out = new ObjectOutputStream(socket.getOutputStream());
					in = new ObjectInputStream(socket.getInputStream());
					Messages msg = (Messages) in.readObject();
					if (msg.equals(Messages.WRITE_DATA)) {
						success = readDataFile(in);
					} else if (msg.equals(Messages.WRITE_CLASS)) {
						success = readClassFile(in);
					} else if (msg.equals(Messages.WRITE_PARTITION)) {
						success = readPartitionFile(in);
					}

				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

				try {
					if (out != null) {
						out.writeBoolean(success);
						out.flush();
					}
				} catch (IOException e) {
					// Ignore.
				}
			}

			private boolean readDataFile(ObjectInputStream in) {
				try {
					String namespace = in.readUTF();
					int blockIndex = in.readInt();
					int numLines = in.readInt();

					File file = new File(createDataFilePath(namespace, blockIndex));
					if (file.exists()) {
						file.delete();
					}
					file.createNewFile();
					FileOutputStream fos = new FileOutputStream(file);
					PrintWriter writer = new PrintWriter(fos);
					for (int i = 0; i < numLines; i++) {
						writer.write(in.readUTF());
						if (i != numLines - 1) {
							writer.write("\n");
						}
					}

					addToLocalFiles(namespace, blockIndex);
					writer.close();
					return true;
				} catch (IOException e) {
					e.printStackTrace();
					return false;
				}
			}

			private boolean readClassFile(ObjectInputStream in) {
				try {
					String namespace = in.readUTF();
					File file = new File(createClassFilePath(namespace));
					if (file.exists()) {
						file.delete();
					}
					file.createNewFile();
					FileOutputStream fos = new FileOutputStream(file);
					BufferedOutputStream bos = new BufferedOutputStream(fos);
					int numBytes = 0;
					while ((numBytes = in.readInt()) > 0) {
						byte[] c = new byte[numBytes];
						in.read(c);
						bos.write(c);
					}
					bos.close();
					return true;
				} catch (IOException e) {
					e.printStackTrace();
					return false;
				}
			}

			private boolean readPartitionFile(ObjectInputStream in) {
				boolean success = false;
				PrintWriter writer = null;
				try {
					int jobId = in.readInt();
					String filename = in.readUTF();
					File newFile = new File(getRoot() + DATA_PATH + filename);
					if (newFile.exists()) {
						newFile.delete();
					}
					newFile.createNewFile();
					writer = new PrintWriter(new FileOutputStream(newFile));
					while (in.readBoolean()) {
						String line = in.readUTF();
						writer.println(line);
					}
					synchronized (partitionFiles) {

						Set<File> files = partitionFiles.get(jobId);
						if (partitionFiles == null) {
							files = new HashSet<File>();
							partitionFiles.put(jobId, files);
						}
						files.add(newFile);
					}

					success = true;
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (writer != null) {
						writer.close();
					}
				}
				return success;
			}
		}
	}

	public class ReadServer extends Thread {

		private ServerSocket serverSocket;

		public ReadServer() throws IOException {
			serverSocket = new ServerSocket(readPort);
		}

		@Override
		public void run() {
			while (true) {
				try {
					Socket socket = serverSocket.accept();
					new ReaderWorker(socket).start();
				} catch (IOException e) {
					// Ignore and keep listening.
				}
			}
		}

		public class ReaderWorker extends Thread {
			private Socket socket;

			public ReaderWorker(Socket socket) {
				this.socket = socket;
			}

			public void run() {
				try {
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

					String filename = in.readUTF();
					File localFile = new File(DATA_PATH + filename);

					if (!localFile.exists()) {
						out.writeBoolean(false);
						out.flush();
						return;
					}
					out.writeBoolean(true);
					out.flush();

					FileInputStream fin = new FileInputStream(localFile);

					byte[] bytes = new byte[1024];
					int numBytesRead = 0;
					while ((numBytesRead = fin.read(bytes)) > 0) {
						out.writeInt(numBytesRead);
						out.write(bytes);
						out.flush();
					}
					out.writeInt(numBytesRead);

					out.flush();
					fin.close();
					socket.close();
					out.close();
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public String createDataFilePath(String filename, int blockIndex) {
		return DATA_PATH + String.format("%s-%d.pt", filename, blockIndex);
	}

	public String createPartitionFilePath(String filename, int jobId, int partitionNo)
		throws RemoteException {
		return DATA_PATH
			+ createFilename(filename, jobId, FileType.PARTITION, partitionNo, manager.getNodeId());
	}

	public String createMappedDataFilePath(String filename, int jobId, int blockIndex)
		throws RemoteException {
		return DATA_PATH
			+ createFilename(filename, jobId, FileType.MAPPED, blockIndex, manager.getNodeId());
	}

	public String createReducerInputFilePath(String filename, int jobId, int partitionNo)
		throws RemoteException {
		return DATA_PATH
			+ createFilename(filename, jobId, FileType.REDUCER_IN, partitionNo, manager.getNodeId());
	}

	public String createReducerOutputFilePath(String filename, int jobId, int partitionNo)
		throws RemoteException {
		return DATA_PATH
			+ createFilename(filename, jobId, FileType.REDUCER_OUT, partitionNo,
				manager.getNodeId());
	}

	public String createFinalOutputFilePath(String filename, int jobId) throws RemoteException {
		return DATA_PATH + createFilename(filename, jobId, FileType.FINAL_OUTPUT);
	}

	// TODO: ONLY USED ONCE.
	public String createClassFilePath(String filename) {
		return String.format("%s%s%s.class", getRoot(), CLASS_PATH, filename);
	}

	// TODO: ONLY USED ONCE.
	public String createFilename(String filename, int jobId, FileType fileType) {
		return String.format("%s-%s-jobId-%d", filename, fileType.toString(), jobId);
	}

	public String createFilename(String filename, int jobId, FileType type, int partNo, int nodeId) {
		return String.format("%s-%s-jobId-%d-part-%d-node-%d", filename, type.toString(), jobId,
			partNo, nodeId);
	}
}
