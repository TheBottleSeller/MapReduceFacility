import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;


public class JobDispatcher extends Thread {
	private volatile Queue<NodeJob> jobs;
	private Thread dispatcher;
	private JobScheduler scheduler;
	
	public JobDispatcher(JobScheduler scheduler) {
		jobs = new LinkedBlockingQueue<NodeJob>();
		dispatcher = this;
		this.scheduler = scheduler;
	}
	
	public void enqueue(NodeJob job) {
		jobs.add(job);
		synchronized (dispatcher) {
			dispatcher.notify();
		}
	}
	
	@Override
	public void run() {
		while(true) {
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
				
			}
		}
	}
}
