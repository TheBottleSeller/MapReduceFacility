import java.rmi.RemoteException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class JobDispatcher extends Thread {
	
	private static final int MAX_QUEUE_SIZE = 1024;
	
	private FacilityManagerMasterImpl master;
	private volatile BlockingQueue<NodeJob> jobs;
	private Thread dispatcher;
	private JobScheduler scheduler;

	public JobDispatcher(FacilityManagerMasterImpl master, Config config) {
		this.master = master;
		dispatcher = this;
		jobs = new ArrayBlockingQueue<NodeJob>(1024);
	}

	public void enqueue(NodeJob job) {
		try {
			jobs.put(job);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while (true) {
			// TODO potential race condition?
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
