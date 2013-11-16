import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class JobScheduler {

	private FacilityManagerMasterImpl master;
	private Config config;
	private Map<Integer, MapReduceProgram> activePrograms;
	private Map<Integer, MapReduceProgram> completedPrograms;
	private AtomicInteger[] activeMaps;
	private AtomicInteger[] activeReduces;
	private AtomicInteger totalJobs;
	private Map<Integer, Set<NodeJob>> activeNodeJobs;

	private JobDispatcher jobDispatcher;
	private HealthChecker healthChecker;

	private int maxMaps;
	private int maxReduces;

	public JobScheduler(FacilityManagerMasterImpl master, Config config) {
		this.master = master;
		this.config = config;
		this.maxMaps = config.getMaxMapsPerHost();
		this.maxReduces = config.getMaxReducesPerHost();
		activePrograms = Collections.synchronizedMap(new HashMap<Integer, MapReduceProgram>());
		completedPrograms = Collections.synchronizedMap(new HashMap<Integer, MapReduceProgram>());
		int numParticipants = config.getParticipantIps().length;
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

	public int findWorker(NodeJob job) {
		int nodeId = -1;
		System.out.println("Finding worker for job " + job);
		if (job instanceof MapJob) {
			nodeId = findMapper((MapJob) job);
		} else if (job instanceof MapCombineJob) {
			nodeId = findMapCombiner((MapCombineJob) job);
		} else if (job instanceof ReduceJob) {
			nodeId = findReducer((ReduceJob) job);
		} else if (job instanceof ReduceCombineJob) {
			nodeId = findReduceCombiner((ReduceCombineJob) job);
		} else {
			System.out.println("Error");
		}
		MapReduceProgram prog = activePrograms.get(job.getId());
		prog.assignJob(job, nodeId);
		return nodeId;
	}

	public int findMapper(MapJob job) {
		try {
			Map<Integer, Set<Integer>> blockLocations = master.getBlockLocations(job.getFilename());
			Set<Integer> nodes = blockLocations.get(job.getBlockIndex());
			nodes.removeAll(getMaxedMappers(nodes));

			if (nodes.isEmpty()) {
				// TODO this is where we send a map job to a mapper without the block
				// needs to getFile it, this should be handled on the job recieving side

				// get the minimum worker across all nodes
				int numParticipants = config.getParticipantIps().length;
				for (int i = 0; i < numParticipants; i++) {
					nodes.add(i);
				}

				// remove maxed mappers again
				nodes.removeAll(getMaxedMappers(nodes));
				if (!nodes.isEmpty()) {
					return findMinWorker(nodes);
				}
			} else {
				// get min worker and run job
				return findMinWorker(nodes);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return -1;
	}

	public int findMapCombiner(MapCombineJob job) {
		int nodeId = job.getNodeId();

		if (!healthChecker.isHealthy(nodeId)) {
			nodeId = -1;
			MapReduceProgram prog = activePrograms.get(job.getId());
			for (int blockIndex : job.getBlockIndices()) {
				MapJob mapJob = prog.getMapJob(blockIndex);
				jobDispatcher.enqueue(mapJob);
			}
		}
		return nodeId;
	}

	public int findReducer(ReduceJob job) {
		Set<Integer> allNodes = new HashSet<Integer>();

		int numParticipants = config.getParticipantIps().length;
		for (int i = 0; i < numParticipants; i++) {
			allNodes.add(i);
		}

		allNodes.removeAll(getMaxedReducers(allNodes));

		if (!allNodes.isEmpty()) {
			return findMinWorker(allNodes);
		}
		return -1;
	}

	public int findReduceCombiner(ReduceCombineJob job) {
		int numParticipants = config.getParticipantIps().length;
		Set<Integer> allNodes = new HashSet<Integer>(numParticipants);
		for (int i = 0; i < numParticipants; i++) {
			allNodes.add(i);
		}
		return findMinWorker(allNodes);
	}

	public Set<Integer> getMaxedMappers(Set<Integer> nodeIds) {
		Set<Integer> maxedNodes = new HashSet<Integer>();
		for (int nodeId : nodeIds) {
			if (getNumMappers(nodeId) >= maxMaps) {
				maxedNodes.add(nodeId);
			}
		}
		return maxedNodes;
	}

	public Set<Integer> getMaxedReducers(Set<Integer> nodeIds) {
		Set<Integer> maxedNodes = new HashSet<Integer>();
		for (int nodeId : nodeIds) {
			if (getNumReducers(nodeId) >= maxReduces) {
				maxedNodes.add(nodeId);
			}
		}
		return maxedNodes;
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

	public int issueJob(Class<?> clazz, String inputFile, int numBlocks) {
		System.out.println("Scheduler issuing job");
		int jobId = totalJobs.getAndIncrement();
		
		MapReduceProgram prog = new MapReduceProgram(jobId, clazz, inputFile, numBlocks,
			config.getParticipantIps().length);
		activePrograms.put(jobId, prog);

		Set<MapJob> mapJobs = prog.createMapJobs();
		for (MapJob mapJob : mapJobs) {
			jobDispatcher.enqueue(mapJob);
		}
		return jobId;
	}

	public void jobFinished(NodeJob job) {
		if (job instanceof MapJob) {
			mapFinished((MapJob) job);
		} else if (job instanceof MapCombineJob) {
			mapCombineFinished((MapCombineJob) job);
		} else if (job instanceof ReduceJob) {
			reduceFinished((ReduceJob) job);
		} else if (job instanceof ReduceCombineJob) {
			reduceCombineFinished((ReduceCombineJob) job);
		} else {
			System.out.println("Error");
		}
	}

	public void mapFinished(MapJob mapJob) {
		int jobId = mapJob.getId();
		MapReduceProgram prog = activePrograms.get(jobId);
		boolean mapPhaseFinished = prog.mapFinished(mapJob);
		if (mapPhaseFinished) {
			System.out.println("MAP PHASE FINISHED");
			// Start combine phase on all of the mappers
			
			// make map of nodeId -> list of blocks mapped on node
			Map<Integer, Set<Integer>> nodeToBlocks = prog.getNodeToBlocks();
			System.out.println(prog.getMaxKey() + " " + prog.getMinKey());
			for (Integer mapperId : nodeToBlocks.keySet()) {
				MapCombineJob mcJob = prog
					.createMapCombineJob(mapperId, nodeToBlocks.get(mapperId));
				jobDispatcher.enqueue(mcJob);
			}
		}
	}

	public void mapCombineFinished(MapCombineJob job) {
		MapReduceProgram prog = activePrograms.get(job.getId());

		boolean combinePhaseFinished = prog.mapCombineFinished(job);
		System.out.println("MapCombineJob finished successfully " + job);
		if (combinePhaseFinished) {
			System.out.println("Creating reduce jobs");
			Set<Integer> mappers = prog.getMappers();
			int numPartitions = prog.getNumPartitions();
			for (int partitionNo = 0; partitionNo < numPartitions; partitionNo++) {
				ReduceJob reduceJob = prog.createReduceJob(partitionNo, mappers);
				jobDispatcher.enqueue(reduceJob);
			}
		}
	}

	public void reduceFinished(ReduceJob job) {
		MapReduceProgram prog = activePrograms.get(job.getId());
		boolean reducePhaseFinished = prog.reduceFinished(job);
		if (reducePhaseFinished) {
			System.out.println("Creating reduce combiner");
			// Gather reduction files, combine them, and upload the results.
			ReduceCombineJob rcJob = prog.createReduceCombineJob();
			jobDispatcher.enqueue(rcJob);
		}
	}
	
	public void reduceCombineFinished(ReduceCombineJob job) {
		MapReduceProgram prog = activePrograms.get(job.getId());
		prog.reduceCombineFinished(job);
		System.out.println("The program has finished");
		activePrograms.remove(prog);
		completedPrograms.put(prog.getId(), prog);
	}
	
	public void nodeDied(int nodeId) {
		for (MapReduceProgram prog : activePrograms.values()) {
			Set<NodeJob> jobs = prog.getAssignments(nodeId);
			for (NodeJob job : jobs) {
				job.setDone(false);
				if (job instanceof MapCombineJob) {
					// TODO STOPPED HERE
				}
				jobDispatcher.enqueue(job);
			}
		}
	}

	public void setDispatcher(JobDispatcher dispatcher) {
		this.jobDispatcher = dispatcher;
	}

	public void setHealthChecker(HealthChecker healthChecker) {
		this.healthChecker = healthChecker;
	}
	
	public synchronized int getNumMappers(int nodeId) {
		return getNumAssignments(nodeId, MapJob.class);
	}

	public synchronized int getNumReducers(int nodeId) {
		return getNumAssignments(nodeId, ReduceJob.class);
	}

	
	public int getNumAssignments(int nodeId, Class<?> clazz) {
		int numMappers = 0;
		for (MapReduceProgram prog : activePrograms.values()) {
			numMappers = prog.getNumAssignments(nodeId, clazz);
		}
		return numMappers;
	}
}
