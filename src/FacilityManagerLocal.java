import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class FacilityManagerLocal extends Thread {

	private static final String PROMPT = "=> ";
	private static String startParticipantScript = "start_participant.sh";

	private FSImpl fs;
	private Config config;
	private boolean isMaster;
	private ServerSocket socketServer;

	public FacilityManagerLocal(Config config) throws IOException {
		this.config = config;
		this.isMaster = true;
		this.fs = new FSImpl(config);
	}

	public FacilityManagerLocal(String masterIp, int port) throws UnknownHostException, IOException {
		this.isMaster = false;
		this.fs = new FSImpl(config);
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
}
