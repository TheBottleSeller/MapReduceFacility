import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class JobDispatcher extends Thread {
	private FacilityManagerMasterImpl master;
	private volatile Queue<NodeJob> jobs;
	private Thread dispatcher;
	private Map<Integer, Set<NodeJob>> activeNodeJobs;
	private JobScheduler scheduler;

	public JobDispatcher(FacilityManagerMasterImpl master, Config config) {
		this.master = master;
		dispatcher = this;
		jobs = new LinkedBlockingQueue<NodeJob>();
		int numParticipants = config.getParticipantIps().length;
		for (int i = 0; i < numParticipants; i++) {
			activeNodeJobs.put(i, Collections.synchronizedSet(new HashSet<NodeJob>()));
		}
	}

	public void enqueue(NodeJob job) {
		jobs.add(job);
		synchronized (dispatcher) {
			dispatcher.notify();
		}
	}

	@Override
	public void run() {
		while (true) {
			// TODO potential race condition?
			NodeJob job = jobs.poll();
			if (job == null) {
				// wait for notify from an enqueue
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				int nodeId = scheduler.findWorker(job);
				if (nodeId == -1) {
					if (job instanceof MapJob || job instanceof ReduceJob) {
						enqueue(job);
					}
				} else {
					try {
						master.getManager(nodeId).runJob(job);
						activeNodeJobs.get(nodeId).add(job);
					} catch (RemoteException e) {
						if (job instanceof MapJob || job instanceof ReduceJob) {
							enqueue(job);
						}
					}
				}
			}
		}
	}

	public void setScheduler(JobScheduler scheduler) {
		this.scheduler = scheduler;
	}
}
