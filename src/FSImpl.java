import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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

public class FSImpl implements FS {

	private static String ROOT_FS_PATH = "~/fs440/";
	private static int WRITE_PORT = 8083;
	private static int READ_PORT = 8084;

	private Map<String, Set<Integer>> localFiles;
	private FacilityManager manager;
	private int blockSize;

	public FSImpl(FacilityManager manager) throws RemoteException {
		localFiles = Collections
				.synchronizedMap(new HashMap<String, Set<Integer>>());
		this.manager = manager;
		blockSize = manager.getConfig().getBlockSize();

		// set up root directory of local fs
		File root = new File(ROOT_FS_PATH);
		if (!root.exists()) {
			root.mkdir();
		} else if (!root.isDirectory()) {
			root.delete();
			root.mkdir();
		}
	}

	@Override
	public void upload(File file, String namespace) throws IOException {
		
		int totalLines = getNumLines(file);
		
		int numBlocks = Math.max(1, (int) Math.ceil(totalLines * 1.0 / manager.getConfig().getBlockSize()));
		
		try {
			Map<Integer, Set<Integer>> blockDistribution = manager
					.distributeBlocks(namespace, totalLines);
			
			// invert the map to get blockIndex -> nodeId map
			Map<Integer, Set<Integer>> blockToNodes = new HashMap<Integer, Set<Integer>>();
			for (int nodeId : blockDistribution.keySet()) {
				for (int blockIndex : blockDistribution.get(nodeId)) {
					Set<Integer> nodes = blockToNodes.get(blockIndex);
					if (nodes == null) {
						nodes = new HashSet<Integer>();
						blockToNodes.put(blockIndex, nodes);
					}
					nodes.add(nodeId);
				}
			}
			
			BufferedReader reader = new BufferedReader(new FileReader(file));
			for (int i = 0; i < numBlocks; i++) {
				Set<Integer> nodes = blockToNodes.get(i);
				int numNodes = nodes.size();
				int numReplicated = 0;
				int numLines = Math.min(blockSize, totalLines - blockSize * i);
				
				for (int nodeId : nodes) {
					boolean success = false;
					int attempts = 0;
					int maxAttempts = manager.getConfig().getParticipantIps().length;
					while (attempts < maxAttempts) {
						// upper bound size of each line to 100 characters
						reader.mark(blockSize*100);
						if (manager.getNodeId() == nodeId) {
							success = localWrite(reader, namespace, i, numLines);
						} else {
							success = remoteWrite(reader, nodeId, namespace, i, numLines);
						}
						if (numReplicated != numNodes - 1) {
							reader.reset();
						}
						if (!success) {
							nodeId = manager.redistributeBlock(nodeId);
							attempts++;
						} else {
							manager.updateFSTable(namespace, i, nodeId);
							break;
						}
					}
					if (success) {
						numReplicated++;
					} else {
						System.out.println("Could not upload file. No participants responding");
					}
				}
			}
			reader.close();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public boolean localWrite(BufferedReader reader, String namespace,
			int blockIndex, int numLines) {
		boolean success = false;
		File file = new File(createFilePath(namespace, blockIndex));
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

	public boolean remoteWrite(BufferedReader reader, int nodeId,
			String namespace, int blockIndex, int numLines) {
		String nodeAddress;
		boolean success = false;
		try {
			nodeAddress = manager.getConfig().getParticipantIps()[nodeId];
			Socket socket = new Socket(nodeAddress, WRITE_PORT);

			ObjectOutputStream out = new ObjectOutputStream(
					socket.getOutputStream());
			out.flush();
			ObjectInputStream in = new ObjectInputStream(
					socket.getInputStream());

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

	@Override
	public File getFile() {
		return null;
	}

	private int getNumLines(File file) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(file));
		try {
			byte[] c = new byte[1024];
			int count = 0;
			int readChars = 0;
			boolean empty = true;
			while ((readChars = is.read(c)) != -1) {
				empty = false;
				for (int i = 0; i < readChars; i++) {
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

	public class Writer extends Thread {
		private ServerSocket serverSocket;

		public Writer() throws IOException {
			serverSocket = new ServerSocket(WRITE_PORT);
		}

		@Override
		public void run() {
			while (true) {
				try {
					Socket socket = serverSocket.accept();
					new WriterWorker(socket).start();
				} catch (IOException e) {
					// ignore and keep listening
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
					String namespace = in.readUTF();
					int blockIndex = in.readInt();
					int numLines = in.readInt();

					File file = new File(createFilePath(namespace, blockIndex));
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
					success = true;
				} catch (IOException e) {
					e.printStackTrace();
				}

				try {
					if (out != null) {
						out.writeBoolean(success);
						out.flush();
					}
				} catch (IOException e) {
					// ignore
				}
			}
		}
	}

	public class Reader {
		public Reader() {

		}
	}

	public String createFilePath(String namespace, int blockIndex) {
		return ROOT_FS_PATH + namespace + "-" + blockIndex + ".pt";
	}
}
