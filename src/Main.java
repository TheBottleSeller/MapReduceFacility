import java.io.File;
import java.io.IOException;

public class Main {
	
	public static FacilityManager master;

	public static void main(String[] args) throws IOException {
		try {
			FacilityManagerLocal facilityManager;
			if (args.length == 2) {
				// FacilityManager should behaves as the master.
				Config config = new Config(new File(args[1]));
				facilityManager = new FacilityManagerMaster(config);
				master = facilityManager;
			} else if (args.length == 4) {
				// FacilityManager should behave as a slave.
				String masterIp = args[1];
				int id = Integer.parseInt(args[2]);
				int port = Integer.parseInt(args[3]);
				facilityManager = new FacilityManagerLocal(masterIp, id, port);
			} else {
				System.out.println("Usage: FacilityManager -m <configFile> for masters and "
					+ "FacilityManager -s <hostname> for participants.");
				return;
			}

			facilityManager.run();
		} catch (Exception e) {

			e.printStackTrace();
			System.exit(0);
		}

	}
}
