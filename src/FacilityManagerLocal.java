import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class FacilityManagerLocal extends Thread implements FacilityManager {

	private static final String PROMPT = "=> ";

	private FSImpl fs;
	private Config config;
	private ServerSocket socketServer;
	private FacilityManagerMaster master;

	public FacilityManagerLocal(Config config) throws IOException {
		this.config = config;
		this.fs = new FSImpl(config);
	}

	public FacilityManagerLocal(String masterIp, int port) throws UnknownHostException,
		IOException, NotBoundException {
		this.fs = new FSImpl(config);		
		this.master = (FacilityManagerMaster) LocateRegistry.getRegistry(1234).lookup("MASTER");
		connectMaster(masterIp, port);
	}

	public void run() {
		Scanner scanner = new Scanner(System.in);
		System.out.print(PROMPT);

		while (scanner.hasNextLine()) {
			String command = scanner.nextLine();
			System.out.println(PROMPT);
			if (command.startsWith("upload")) {
				// Get the filename and namespace.
				int fileNameStart = command.indexOf(" ") + 1;
				int nameSpaceStart = command.lastIndexOf(" ") + 1;
				File file = new File(command.substring(fileNameStart, nameSpaceStart - 2));
				if (file.exists()) {
					String namespace = command.substring(nameSpaceStart);
					fs.upload(file, namespace);
				} else {
					System.out.println("Error: File does not exist.");
					System.out.println(PROMPT);
				}
			}
		}
	}

	public void connectMaster(String masterIp, int port) throws UnknownHostException, IOException {
		Socket s = new Socket(masterIp, port);
		ObjectInputStream in = new ObjectInputStream(s.getInputStream());
		try {
			Config config = (Config) in.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public Config getConfig() {
		return config;
	}

	@Override
	public Map<Integer, Set<Integer>> distributeBlocks() throws RemoteException {
		return null;
	}
}
