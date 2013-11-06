import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class JobScheduler {
	
	private FacilityManagerMaster master;
	private Map<Integer, AtomicInteger> activeMaps;
	private Map<Integer, AtomicInteger> activeReduces;
	private AtomicInteger totalJobs;
	
	public JobScheduler(FacilityManagerMaster master) {
		this.master = master;
		activeMaps = Collections.synchronizedMap(new HashMap<Integer, AtomicInteger>());
		activeReduces = Collections.synchronizedMap(new HashMap<Integer, AtomicInteger>());
		int numParticipants = master.getConfig().getParticipantIps().length;
		for (int i = 0; i < numParticipants; i++) {
			activeMaps.put(i, new AtomicInteger(0));
			activeReduces.put(i, new AtomicInteger(0));
		}
		totalJobs = new AtomicInteger(0);
	}
	
	public List<Job> issueJob(Class<?> clazz, String inputFile, Map<Integer, Set<Integer>> blockLocations) {
		int jobId = totalJobs.getAndIncrement();
		Job job = new Job(jobId, inputFile);
		for (int blockIndex : blockLocations.keySet()) {
			Set<Integer> nodeIds = blockLocations.get(blockIndex);
			int minWork = Integer.MAX_VALUE;
			int minWorker = -1;
			for (int nodeId : nodeIds) { 
				int work = activeMaps.get(nodeId).get() + activeReduces.get(nodeId).get();
				if (work < minWork) {
					minWork = work;
					minWorker = nodeId;
				}
			}
			job.addMapper(minWorker, blockIndex);
			activeMaps.get(minWorker).incrementAndGet();
		}
		
		Map<Integer, Integer> mappers = job.getMappers();
		for (Integer nodeId : mappers.keySet()) {
			int blockIndex = mappers.get(nodeId);
			// need to know send the map job to each associated node
			// with the filename, block index, and job id
		}
		try {
			MapReduce440 c = (MapReduce440) clazz.newInstance();
			//Mapper440 mapper = c.getMapper();
			//Reducer440 reducer = c.getReducer();
			//System.out.println("mapper = " + mapper);
			//System.out.println("reducer = " + reducer);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}
}
