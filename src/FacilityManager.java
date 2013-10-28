import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class FacilityManager extends Thread {

	private static String startParticipantScript = "start_participant.sh";
	private Config config;
	private boolean isMaster;
	private static final String PROMPT = "=> ";
	private ServerSocket socketServer;

	public FacilityManager(Config config) throws IOException {
		this.config = config;
		this.isMaster = true;
		Server server = new Server(config.getFsPort());
		server.start();
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
		}
	}

	public void connectParticipants() throws IOException {
		String localAddress = InetAddress.getLocalHost().getHostAddress();
		for (String slaveIp : config.getParticipantIps()) {
			// execute script
			String command = "./" + startParticipantScript;
			ProcessBuilder pb = new ProcessBuilder(command,
					slaveIp, localAddress, "" + config.getFsPort());
			pb.directory(null);
			Process p = pb.start();
		}
	}
	
	public void connectMaster(String masterIp, int port) throws UnknownHostException, IOException {
		Socket s = new Socket(masterIp, port);
		ObjectInputStream in = (ObjectInputStream) s.getInputStream();
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
					System.out.println("Slave connected");
					new Thread(new Runnable() {
						public void run() {
							ObjectOutputStream out;
							try {
								out = (ObjectOutputStream) s.getOutputStream();
								out.writeObject(config);
							} catch (IOException e1) {
								e1.printStackTrace();
							}
						}
					}).start();
				} catch (IOException e) {
					
				}
			}
		}
		
		
	}

}
