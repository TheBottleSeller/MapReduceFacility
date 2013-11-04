import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;

public class Main {

	public static FacilityManager master;

	public static void main(String[] args) throws IOException, NotBoundException,
		AlreadyBoundException, InterruptedException {
		FacilityManagerLocal facilityManager;
		if (args.length == 2) {
			// FacilityManager should behaves as the master.
			Config config = new Config(new File(args[1]));
			facilityManager = new FacilityManagerMaster(config);
			master = facilityManager;
			facilityManager.run();
		} else if (args.length == 4) {
			// FacilityManager should behave as a slave.
			String masterIp = args[1];
			int id = Integer.parseInt(args[2]);
			int port = Integer.parseInt(args[3]);
			facilityManager = new FacilityManagerLocal(masterIp, id, port);
			while (true) {
				Thread.sleep(1000);
			}
		} else {
			System.out.println("Usage: FacilityManager -m <configFile> for masters and "
				+ "FacilityManager -s <hostname> for participants.");
			return;
		}
	}

	public void createExceptionHandler() {
		Thread.currentThread().setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread th, Throwable t) {
				File log = new File("error.log");
				if (!log.exists()) {
					try {
						log.createNewFile();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				try {
					PrintWriter writer = new PrintWriter(new FileWriter(log));
					t.printStackTrace(writer);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
	}
}
