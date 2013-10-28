import java.io.IOException;
import java.net.InetAddress;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.Set;

public class FacilityManagerMaster extends FacilityManagerLocal implements FacilityManager {

	private static String startParticipantScript = "start_participant.sh";

	public FacilityManagerMaster(Config config) throws IOException, AlreadyBoundException {
		super(config);
		Registry r = LocateRegistry.createRegistry(1234);
		r.bind("MASTER", UnicastRemoteObject.exportObject(this, 0));
		connectParticipants();
	}
	
	public void connectParticipants() throws IOException {
		String localAddress = InetAddress.getLocalHost().getHostAddress();
		for (String slaveIp : getConfig().getParticipantIps()) {
			// Execute script.
			String command = "./" + startParticipantScript;
			ProcessBuilder pb = new ProcessBuilder(command, slaveIp, localAddress, ""
				+ getConfig().getFsPort());
			pb.directory(null);
			pb.start();
		}
	}

	@Override
	public Map<Integer, Set<Integer>> distributeBlocks() throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

}
