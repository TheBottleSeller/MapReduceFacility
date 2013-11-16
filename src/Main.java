import java.io.File;

public class Main {

	public static FacilityManager master;

	public static void main(String[] args) throws Exception {
		FacilityManagerImpl facilityManager;
		if (args.length == 2) {
			// FacilityManager should behaves as the master.
			Config config = new Config(new File(args[1]));
			facilityManager = new FacilityManagerMasterImpl(config);
			master = facilityManager;
			facilityManager.run();
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
				.println("Usage: -m <configFile> for masters and -s <hostname> -p <port> for participants.");
			return;
		}
	}
}
