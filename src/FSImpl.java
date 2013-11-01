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
		
		int numLines = getNumLines(file);
		if (numLines == 0) {
			return;
		}
		
		int numBlocks = (int) Math.ceil(numLines * 1.0 / manager.getConfig().getBlockSize());
		
		try {
			Map<Integer, Set<Integer>> blockDistribution = manager
					.distributeBlocks(namespace, numLines);
			
			// invert the map to get blockIndex -> nodeId map
			Map<Integer, Integer> blockToNode = new HashMap<Integer, Integer>();
			for (int nodeId : blockDistribution.keySet()) {
				for (int blockIndex : blockDistribution.get(nodeId)) {
					blockToNode.put(blockIndex, nodeId);
				}
			}
			
			BufferedReader reader = new BufferedReader(new FileReader(file));
			for (int i = 0; i < numBlocks; i++) {
				int nodeId = blockToNode.get(i);
				if (manager.getNodeId() == nodeId) {
					localWrite(reader, namespace, i);
				} else {
					remoteWrite(reader, nodeId, i);
				}
			}
			is.close();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	public void localWrite(BufferedReader reader, String namespace, int blockIndex) throws IOException {
		File file = new File(createFilePath(namespace, blockIndex));
		if (file.exists()) {
			file.delete();
		}
		file.createNewFile();
		
		FileOutputStream fos = new FileOutputStream(file);
		PrintWriter writer = new PrintWriter(fos);
		for (int i = 0; i < blockSize; i++) {
			String record = reader.readLine();
			// check if EOF
			if (record == null) {
				break;
			} else {
				writer.write(record);
				if (i != blockSize - 1) {
					writer.write('\n');
				}
			}
		}
		writer.close();
		addToLocalFiles(namespace, blockIndex);
	}
	
	public void remoteWrite(BufferedReader reader, )
	
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
	
	public class Writer extends Thread{
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
		
		public void remoteWrite(String nodeIp, File file, int blockIndex) {
			
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
