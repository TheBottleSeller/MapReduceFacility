import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HealthChecker extends Thread {

	private static int HEALTH_CHECK_INTERVAL = 2000; // check every 2 seconds
	private String[] participantIps;
	private FacilityManagerMaster master;
	private Map<Integer, FacilityManager> slaves;

	public HealthChecker(FacilityManagerMaster master) {
		this.master = master;
		this.participantIps = master.getConfig().getParticipantIps();
		slaves = Collections.synchronizedMap(new HashMap<Integer, FacilityManager>());
	}

	@Override
	public void run() {
		while (true) {
			for (int i = 0; i < participantIps.length; i++) {
				FacilityManager slaveManager = slaves.get(i);
				if (slaveManager != null) {
					boolean heartbeat = false;
					try {
						heartbeat = slaveManager.heartBeat();
					} catch (RemoteException e) {
						e.printStackTrace();
						heartbeat = false;
					}

					// check if the slave is dead
					if (!heartbeat) {
						// if the slave is dead, remove itself from the list of participants
						// and notify the master manager
						slaves.remove(i);
						master.slaveDied(i);
					}
				}
			}
			try {
				Thread.sleep(HEALTH_CHECK_INTERVAL);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public synchronized void addConnection(int id, FacilityManager slaveManager) {
		slaves.put(id, slaveManager);
	}

	public boolean isHealthy(int id) {
		if (id == master.getNodeId()) {
			return true;
		}
		return slaves.get(id) != null;
	}
}
