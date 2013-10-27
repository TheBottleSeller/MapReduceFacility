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
					facilityManager = new FacilityManager();
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

		ProcessBuilder pb = new ProcessBuilder("myshellScript.sh", "myArg1", "myArg2");
		Map<String, String> env = pb.environment();
		env.put("VAR1", "myValue");
		env.remove("OTHERVAR");
		env.put("VAR2", env.get("VAR1") + "suffix");
		pb.directory(new File("myDir"));
		Process p = pb.start();
	}
}
