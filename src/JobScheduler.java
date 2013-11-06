import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class JobScheduler {
	
	private Map<Integer, FacilityManager> managers;
	private Map<Integer, Job> activeJobs;
	private Map<Integer, Job> completedJobs;
	private AtomicInteger[] activeMaps;
	private AtomicInteger[] activeReduces;
	private AtomicInteger totalJobs;
	
	public JobScheduler(Map<Integer, FacilityManager> managers, int numParticipants) {
		this.managers = managers;
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
	
	public List<Job> issueJob(Class<?> clazz, String inputFile, Map<Integer, Set<Integer>> blockLocations) {
		int jobId = totalJobs.getAndIncrement();
		int numBlocks = blockLocations.size();
		Job job = new Job(jobId, inputFile, numBlocks);
		for (int blockIndex : blockLocations.keySet()) {
			Set<Integer> nodeIds = blockLocations.get(blockIndex);
			int minWork = Integer.MAX_VALUE;
			int minWorker = -1;
			for (int nodeId : nodeIds) { 
				int work = getNumMappers(nodeId) + getNumReducers(nodeId);
				if (work < minWork) {
					minWork = work;
					minWorker = nodeId;
				}
			}
			job.addMapper(minWorker, blockIndex);
			incrementActiveMaps(minWorker);
		}
		
		activeJobs.put(jobId, job);
		
		for (int blockIndex = 0; blockIndex < numBlocks; blockIndex++) {
			int nodeId = job.getMapper(blockIndex);
			FacilityManager manager = managers.get(nodeId);
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
		return null;
	}

	public boolean mapFinished(int jobId, int nodeId, int blockIndex) {
		return activeJobs.get(jobId).mapFinished();
	}
}
