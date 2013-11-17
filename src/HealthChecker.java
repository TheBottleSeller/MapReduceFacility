import java.rmi.RemoteException;

public class HealthChecker extends Thread {

	private static int HEALTH_CHECK_INTERVAL = 000; // check every 2 seconds
	private FacilityManagerMasterImpl master;
	private boolean[] healthy;
	private int numParticipants;

	public HealthChecker(FacilityManagerMasterImpl master, int numParticipants) {
		this.master = master;
		this.numParticipants = numParticipants;
		this.healthy = new boolean[numParticipants];
	}

	@Override
	public void run() {
		while (true) {
			for (int i = 0; i < numParticipants; i++) {
				FacilityManager slaveManager = master.getManager(i);
				if (slaveManager == null) {
					continue;
				}
				boolean heartbeat = false;
				try {
					heartbeat = slaveManager.heartBeat();
				} catch (RemoteException e) {
					heartbeat = false;
				}

				// check if the slave is dead
				if (heartbeat) {
					healthy[i] = true;
				} else {
					healthy[i] = false;
					master.slaveDied(i);
				}
			}
			try {
				Thread.sleep(HEALTH_CHECK_INTERVAL);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean isHealthy(int id) {
		return healthy[id];
	}
}
