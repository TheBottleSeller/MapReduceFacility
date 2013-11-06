import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class JobScheduler {
	
	private Map<Integer, FacilityManager> managers;
	private AtomicInteger[] activeMaps;
	private AtomicInteger[] activeReduces;
	private AtomicInteger totalJobs;
	
	public JobScheduler(Map<Integer, FacilityManager> managers, int numParticipants) {
		this.managers = managers;
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
		Job job = new Job(jobId, inputFile);
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
		
		Map<Integer, Integer> mappers = job.getMappers();
		for (Integer nodeId : mappers.keySet()) {
			int blockIndex = mappers.get(nodeId);
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
}
