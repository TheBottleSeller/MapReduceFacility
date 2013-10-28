import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class FacilityManager extends Thread {

	private static final String PROMPT = "=> ";
	private static String startParticipantScript = "start_participant.sh";
	
	private FSImpl fs;
	private Config config;
	private boolean isMaster;
	private ServerSocket socketServer;

	public FacilityManager(Config config) throws IOException {
		this.config = config;
		this.isMaster = true;
		this.fs = new FSImpl(config);
		new Server(config.getFsPort()).start();
		connectParticipants();
	}

	public FacilityManager(String masterIp, int port) throws UnknownHostException, IOException {
		this.isMaster = false;
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

	public void connectParticipants() throws IOException {
		String localAddress = InetAddress.getLocalHost().getHostAddress();
		for (String slaveIp : config.getParticipantIps()) {
			// Execute script.
			String command = "./" + startParticipantScript;
			ProcessBuilder pb = new ProcessBuilder(command, slaveIp, localAddress, ""
				+ config.getFsPort());
			pb.directory(null);
			Process p = pb.start();
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

	public class Server extends Thread {
		private ServerSocket socket;

		public Server(int port) throws IOException {
			socket = new ServerSocket(port);
		}

		public void run() {
			while (true) {
				try {
					final Socket s = socket.accept();
					System.out.println("Slave connected.");
					new Thread(new Runnable() {
						public void run() {
							ObjectOutputStream out;
							try {
								out = new ObjectOutputStream(s.getOutputStream());
								out.writeObject(config);
								out.flush();
							} catch (IOException e1) {
								e1.printStackTrace();
							}
						}
					}).start();
				} catch (IOException e) {
					// Ignore.
				}
			}
		}

	}

}
