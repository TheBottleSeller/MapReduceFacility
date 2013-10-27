import java.io.File;
import java.io.IOException;
import java.util.Map;

public class Main {

	public static void main(String[] args) throws IOException {
		try {
			FacilityManager facilityManager;
			if (args.length == 2 && (args[0].equals("-m") || args[0].equals("-s"))) {
				if (args[0].equals("-m")) {
					// FacilityManager should behaves as the master.
					Config config = new Config(new File(args[1]));
					facilityManager = new FacilityManager(config);
				} else {
					// FacilityManager should behave as a slave.
					String masterIp = args[1];
					facilityManager = new FacilityManager();
				}
			} else {
				System.out.println("Usage: FacilityManager -m <configFile> for masters and " +
						"FacilityManager -s <hostname> for participants");
				return;
			}

			facilityManager.run();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

	
	}
}
