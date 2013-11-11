import java.io.FileNotFoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class JobScheduler {

	private FacilityManagerMasterImpl master;
	private Map<Integer, MapReduceJob> activeJobs;
	private Map<Integer, MapReduceJob> completedJobs;
	private AtomicInteger[] activeMaps;
	private AtomicInteger[] activeReduces;
	private AtomicInteger totalJobs;
	private Map<Integer, Set<NodeJob>> activeNodeJobs;

	public JobScheduler(FacilityManagerMasterImpl master, int numParticipants) {
		this.master = master;
		activeJobs = Collections.synchronizedMap(new HashMap<Integer, MapReduceJob>());
		completedJobs = Collections.synchronizedMap(new HashMap<Integer, MapReduceJob>());
		activeMaps = new AtomicInteger[numParticipants];
		activeReduces = new AtomicInteger[numParticipants];
		activeNodeJobs = Collections.synchronizedMap(new HashMap<Integer, Set<NodeJob>>());
		for (int i = 0; i < numParticipants; i++) {
			activeMaps[i] = new AtomicInteger(0);
			activeReduces[i] = new AtomicInteger(0);
			activeNodeJobs.put(i, new HashSet<NodeJob>());
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
	
	public int findMinWorker(Set<Integer> nodeIds) {
		int minWork = Integer.MAX_VALUE;
		int minWorker = -1;
		for (int nodeId : nodeIds) {
			if (master.isNodeHealthy(nodeId)) {
				int work = getNumMappers(nodeId) + getNumReducers(nodeId);
				if (work < minWork) {
					minWork = work;
					minWorker = nodeId;
				}
			}
		}
		return minWorker;
	}

	public int issueJob(Class<?> clazz, String inputFile, Map<Integer, Set<Integer>> blockLocations) {
		System.out.println("Scheduler issuing job");
		int jobId = totalJobs.getAndIncrement();
		int numBlocks = blockLocations.size();
		MapReduceJob job = new MapReduceJob(jobId, clazz, inputFile, numBlocks);
		for (int blockIndex : blockLocations.keySet()) {
			int minWorker = findMinWorker(blockLocations.get(blockIndex)); 
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
				MapJob mapJob = job.createMapJob(blockIndex);
				activeNodeJobs.get(nodeId).add(mapJob);
				success = manager.runMapJob(mapJob);
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

	public void mapFinished(int jobId, int nodeId, int blockIndex, int maxKey, int minKey) {
		boolean mapPhaseFinished = activeJobs.get(jobId).mapFinished(maxKey, minKey);
		if (mapPhaseFinished) {
			// Start combine phase on all of the mappers
			MapReduceJob job = activeJobs.get(jobId);
			Map<Integer, Set<Integer>> nodeToBlocks = new HashMap<Integer, Set<Integer>>(
				job.getNumBlocks());
			for (int i = 0; i < job.getNumBlocks(); i++) {
				int mapperId = job.getMapper(i);
				Set<Integer> blocks = nodeToBlocks.get(mapperId);
				if (blocks == null) {
					blocks = new HashSet<Integer>();
					nodeToBlocks.put(mapperId, blocks);
				}
				blocks.add(i);
			}
			for (Integer mapperId : nodeToBlocks.keySet()) {
				FacilityManager mapper = master.getManager(mapperId);
				if (mapper == null) {
					// TODO what happens if the mapper is null, need to redo map
					// job on another machine
				} else {
					boolean success = false;
					try {
						mapper.runCombineJob(nodeToBlocks.get(mapperId), job.getFilename(), jobId,
							job.getMaxKey(), job.getMinKey(), job.getNumBlocks());
						success = true;
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (!success) {
						// TODO what happens now? again try to redo the map job
						// on another node
					}
				}
			}
		}
	}

	public void combineFinished(int jobId, int combinedBlocks) {
		if (combinedBlocks < 1) {
			// TODO handle this case
			return;
		}
		MapReduceJob job = activeJobs.get(jobId);
		boolean combinePhaseFinished = job.combineFinished(combinedBlocks);
		if (combinePhaseFinished) {
			System.out.println("Scheduler issuing reduces");

			// distribute reducers amongst participants
			Map<Integer, Set<Integer>> blockLocations = master.getBlockLocations(job.getFilename());
			for (int blockIndex : blockLocations.keySet()) {
				int minWorker = findMinWorker(blockLocations.get(blockIndex)); 
				if (minWorker == -1) {
					System.out.println("Could not find worker for block " + blockIndex);
					// TODO: Reset minWorker by transferring block...
				}
				System.out.println("Found worker " + minWorker);
				job.addReducer(minWorker, blockIndex);
				incrementActiveReduces(minWorker);
			}

			// get all mappers ids
			System.out.println("Telling mappers about reducer distribution");
			Set<Integer> mappers = new HashSet<Integer>();
			for (int mapperId : job.getMappers()) {
				mappers.add(mapperId);
			}

			// issue reduce jobs to each reducer
			System.out.println("Scheduled reducers");
			int numPartitions = blockLocations.size();
			Class<?> clazz = job.getUserDefinedClass();
			for (int partitionNo = 0; partitionNo < numPartitions; partitionNo++) {
				int nodeId = job.getReducer(partitionNo);
				System.out.println("Issued node " + nodeId + " with reduce for partition "
					+ partitionNo);
				FacilityManager manager = master.getManager(nodeId);
				boolean success = false;
				try {
					success = manager.runReduceJob(new ReduceJob(jobId, job.getFilename(), partitionNo, mappers, clazz));
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				if (!success) {
					// TODO: What happens here..?
				}
			}
			System.out.println("running map jobs");
		}
	}

	public void reduceFinished(int jobId) throws FileNotFoundException, RemoteException {
		MapReduceJob job = activeJobs.get(jobId);
		boolean reducePhaseFinished = job.reduceFinished();
		if (reducePhaseFinished) {
			System.out.println("Start combining reduces.");

			// Find minimum worker.
			Set<Integer> allNodeIds = new HashSet<Integer>(activeMaps.length);
			for (int id = 0; id < activeMaps.length; id++) {
				allNodeIds.add(id);
			}
			int minWorker = findMinWorker(allNodeIds);
			if (minWorker == -1) {
				System.out.println("Could not find worker to combine reduces.");
				// TODO: Wait? 
			}
			System.out.println("Found worker to combine reduces: " + minWorker);
			
			// Gather reduction files, combine them, and upload the results.
			master.getManager(minWorker).combineReduces(job);
		}
	}
}
