import java.rmi.RemoteException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class JobDispatcher extends Thread {
	
	private FacilityManagerMasterImpl master;
	private volatile BlockingQueue<NodeJob> jobs;
	private JobScheduler scheduler;

	public JobDispatcher(FacilityManagerMasterImpl master, Config config) {
		this.master = master;
		jobs = new LinkedBlockingQueue<NodeJob>();
	}

	public synchronized void enqueue(NodeJob job) {
		try {
			jobs.put(job);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while (true) {
			NodeJob job = null;
			try {
				job = jobs.take();
				int nodeId = scheduler.findWorker(job);
				if (nodeId == -1) {
					if (job instanceof MapJob || job instanceof ReduceJob) {
						enqueue(job);
					}
				} else {
					try {
						master.getManager(nodeId).runJob(job);
					} catch (RemoteException e) {
						if (job instanceof MapJob || job instanceof ReduceJob) {
							enqueue(job);
						}
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void setScheduler(JobScheduler scheduler) {
		this.scheduler = scheduler;
	}
}
