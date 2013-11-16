import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class MainSlave {

	public static FacilityManager master;

	public static void main(String[] args) throws Exception {
		if (args.length == 4) {
			Registry registry = LocateRegistry.getRegistry(Integer.parseInt(args[2]));
			FacilityManagerImpl facilityManager = (FacilityManagerImpl) registry.lookup(args[4]
				+ FacilityManagerImpl.REGISTRY_SLAVE_KEY);
			if (facilityManager == null) {
				System.out.println("Invalid port and clustername.");
			} else {
				facilityManager.run();
			}
		} else {
			System.out.println("Usage: -p <port> -c <clustername>.");
			return;
		}
	}
}
