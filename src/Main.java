import java.io.File;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

public class Main {

	private static final String PROMPT = "=> ";
	public static FacilityManager master;

	public static void main(String[] args) throws Exception {
		FacilityManager facilityManager;
		if (args.length == 2) {
			// FacilityManager should behaves as the master.
			Config config = new Config(new File(args[1]));
			facilityManager = new FacilityManagerMasterImpl(config);
			master = facilityManager;
			displayPrompt(facilityManager);
		} else if (args.length == 4) {
			// Run command prompt from participant.
			Registry registry = LocateRegistry.getRegistry(Integer.parseInt(args[1]));
			facilityManager = (FacilityManager) registry.lookup(args[3]
				+ FacilityManagerImpl.REGISTRY_SLAVE_KEY);
			if (facilityManager == null) {
				System.out.println("Invalid port and clustername.");
			} else {
				displayPrompt(facilityManager);
			}
		} else if (args.length == 5) {
			// FacilityManager should behave as a participant.
			String masterIp = args[1];
			int id = Integer.parseInt(args[2]);
			int port = Integer.parseInt(args[3]);
			String clusterName = args[4];
			boolean successfullyLaunched = false;
			try {
				facilityManager = new FacilityManagerImpl(masterIp, id, port, clusterName);
				successfullyLaunched = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (successfullyLaunched) {
				while (true) {
					Thread.sleep(1000);
				}
			}
		} else {
			System.out
				.println("Usage: -m <configFile> for masters and -s <hostname> <id> <port> <clustername> for participants.");
			return;
		}
	}

	public static void displayPrompt(FacilityManager facilityManager) throws RemoteException {
		Scanner scanner = new Scanner(System.in);
		System.out.print(PROMPT);
		while (scanner.hasNextLine()) {
			String command = scanner.nextLine();
			if (command.equals("exit") && !(facilityManager instanceof FacilityManagerMaster)) {
				break;
			}
			String result = facilityManager.runCommand(command);
			if (result != null) {
				System.out.println(result);
			}
			System.out.print(PROMPT);
		}
	}
}
