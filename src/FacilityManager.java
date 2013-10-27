import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Scanner;

public class FacilityManager extends Thread {

	private Config config;
	private boolean master;
	private static final String PROMPT = "=> ";
	private ServerSocket socketServer;

	public FacilityManager(Config config) {
		this.config = config;
		this.master = true;
	}

	public FacilityManager() {
		this.master = false;

	}

	public void run() {
		try {
			connectParticipants();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Scanner scanner = new Scanner(System.in);
		System.out.print(PROMPT);

		while (scanner.hasNextLine()) {
			String command = scanner.nextLine();
		}
	}

	public void connectParticipants() throws IOException {
		for (String slaveIp : config.getParticipantIps()) {
			ProcessBuilder pb = new ProcessBuilder("start_participant.sh",
					slaveIp, InetAddress.getLocalHost().getHostAddress());
			pb.directory(null);
			Process p = pb.start();
		}
	}

}
