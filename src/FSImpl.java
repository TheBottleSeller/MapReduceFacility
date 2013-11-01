import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.util.Map;
import java.util.Set;

public class FSImpl implements FS {
	
	private static String ROOT_FS_PATH = "~/fs440/";
	private static int WRITE_PORT = 8083;
	private static int READ_PORT = 8084;

	private Map<String, Set<Integer>> localFiles;
	private int lastNode;
	private FacilityManager manager;

	public FSImpl(FacilityManager manager) {
		localFiles = Collections
				.synchronizedMap(new HashMap<String, Set<Integer>>());
		this.manager = manager;
		this.lastNode = 0;
		
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
	public void upload(File file, String namespace) {
		int numLines = -1;
		try {
			numLines = getNumLines(file);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			Map<Integer, Set<Integer>> blockDistribution = manager
					.distributeBlocks(namespace, numLines);
			for (int nodeId : blockDistribution.keySet()) {
				if (manager.getNodeId() == nodeId) {
					localFiles.put(namespace, blockDistribution.get(nodeId));

				} else {
					// send 
				}
			}
		} catch (RemoteException e) {
			e.printStackTrace();
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
				ObjectOutputStream out;
				ObjectInputStream in;
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
					}
					
				} catch (IOException e) {
					e.printStackTrace();
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
