import java.io.IOException;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

public class FacilityManagerMaster extends FacilityManagerLocal implements FacilityManager {

	private static String startParticipantScript = "start_participant.sh";

	public FacilityManagerMaster(Config config, int port) throws IOException {
		super(config);
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
