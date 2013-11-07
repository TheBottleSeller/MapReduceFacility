import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class JobScheduler {
	
	private FacilityManagerMasterImpl master;
	private Map<Integer, Job> activeJobs;
	private Map<Integer, Job> completedJobs;
	private AtomicInteger[] activeMaps;
	private AtomicInteger[] activeReduces;
	private AtomicInteger totalJobs;
	
	public JobScheduler(FacilityManagerMasterImpl master, int numParticipants) {
		this.master = master;
		activeJobs = Collections.synchronizedMap(new HashMap<Integer, Job>());
		completedJobs = Collections.synchronizedMap(new HashMap<Integer, Job>());
		activeMaps = new AtomicInteger[numParticipants];
		activeReduces = new AtomicInteger[numParticipants];
		for (int i = 0; i < numParticipants; i++) {
			activeMaps[i] = new AtomicInteger(0);
			activeReduces[i] = new AtomicInteger(0);
		}
		totalJobs = new AtomicInteger(0);
	}
	
	public synchronized int getNumMappers(int nodeId) {
		return activeMaps[nodeId].get();
	}
	
	public synchronized int getNumReducers(int nodeId) {
		return activeReduces[nodeId].get();
	}
	
	public synchronized void incrementActiveMaps(int nodeId) {
		activeMaps[nodeId].incrementAndGet();
	}
	
	public synchronized void incrementActiveReduces(int nodeId) {
		activeReduces[nodeId].incrementAndGet();
	}
	
	public int issueJob(Class<?> clazz, String inputFile, Map<Integer, Set<Integer>> blockLocations) {
		System.out.println("Scheduler issuing job");
		int jobId = totalJobs.getAndIncrement();
		int numBlocks = blockLocations.size();
		Job job = new Job(jobId, inputFile, numBlocks);
		for (int blockIndex : blockLocations.keySet()) {
			System.out.println("Assigning node for block " + blockIndex);
			Set<Integer> nodeIds = blockLocations.get(blockIndex);
			System.out.println("Possible nodes " + Arrays.toString(nodeIds.toArray()));
			int minWork = Integer.MAX_VALUE;
			int minWorker = -1;
			for (int nodeId : nodeIds) {
				System.out.println("Checking node " + nodeId);
				if (master.isNodeHealthy(nodeId)) {
					System.out.println("Node is healthy");
					int work = getNumMappers(nodeId) + getNumReducers(nodeId);
					if (work < minWork) {
						minWork = work;
						minWorker = nodeId;
					}
				}
			}
			if (minWorker == -1) {
				System.out.println("Could not find worker for block " + blockIndex);
				return -1;
			}
			System.out.println("Found worker " + minWorker);
			job.addMapper(minWorker, blockIndex);
			incrementActiveMaps(minWorker);
		}
		System.out.println("Scheduled mappers");
		activeJobs.put(jobId, job);
		
		for (int blockIndex = 0; blockIndex < numBlocks; blockIndex++) {
			int nodeId = job.getMapper(blockIndex);
			System.out.println("Issued node " + nodeId + " with map for block " + blockIndex);
			FacilityManager manager = master.getManager(nodeId);
			boolean success = false;
			try {
				success = manager.runMapJob(jobId, inputFile, blockIndex, clazz);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			if (!success) {
				// TODO: What happens here..?
			}
		}
		System.out.println("running map jobs");
		return jobId;
	}

	public boolean mapFinished(int jobId, int nodeId, int blockIndex) {
		return activeJobs.get(jobId).mapFinished();
	}
}
