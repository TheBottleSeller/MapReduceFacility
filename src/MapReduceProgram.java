import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapReduceProgram {

	private int id;
	private String filename;
	private Class<?> clazz;
	private int numBlocks;
	private int numParticipants;
	private int completedMaps;
	private int completedCombines;
	private int completedReduces;
	private int maxKey;
	private int minKey;
	private Map<Integer, Set<NodeJob>> jobAssignments;
	private Set<MapJob> mapJobs;
	private Set<MapCombineJob> mapCombineJobs;
	private Set<ReduceJob> reduceJobs;
	private ReduceCombineJob reduceCombineJob;

	public MapReduceProgram(int id, Class<?> clazz, String filename, int numBlocks, int numParticipants) {
		this.id = id;
		this.filename = filename;
		this.clazz = clazz;
		this.numBlocks = numBlocks;
		this.numParticipants = numParticipants;
		completedMaps = 0;
		completedCombines = 0;
		completedReduces = 0;
		maxKey = Integer.MIN_VALUE;
		minKey = Integer.MAX_VALUE;
		mapJobs = Collections.synchronizedSet(new HashSet<MapJob>());
		mapCombineJobs = Collections.synchronizedSet(new HashSet<MapCombineJob>());
		reduceJobs =  Collections.synchronizedSet(new HashSet<ReduceJob>());
		
		jobAssignments = Collections.synchronizedMap(new HashMap<Integer, Set<NodeJob>>());
		for (int i = -1; i < numParticipants; i++) {
			jobAssignments.put(i, Collections.synchronizedSet(new HashSet<NodeJob>()));
		}
	}
	
	public void assignJob(NodeJob job, int nodeId) {
		jobAssignments.get(job.getNodeId()).remove(job);
		jobAssignments.get(nodeId).add(job);
	}
	
	public Set<Integer> getMappers() {
		Set<Integer> mappers = new HashSet<Integer>();
		for (int i = 0; i < numParticipants; i++) {
			Set<NodeJob> assignments = jobAssignments.get(i);
			for (NodeJob job : assignments) {
				if (job instanceof MapJob) {
					mappers.add(i);
					break;
				}
			}
		}
		return mappers;
	}
	
	public int[] getPartitionReducers() {
		int[] partitionReducers = new int[getNumPartitions()];
		for (int i = 0; i < numParticipants; i++) {
			Set<NodeJob> assignments = jobAssignments.get(i);
			for (NodeJob job : assignments) {
				if (job instanceof ReduceJob) {
					partitionReducers[((ReduceJob) job).getPartitionNum()] = i;
				}
			}
		}
		return partitionReducers;
	}

	public MapJob createMapJob(int blockIndex) {
		MapJob job = new MapJob(id, -1, filename, blockIndex, clazz);
		jobAssignments.get(-1).add(job);
		return job;
	}

	public MapCombineJob createMapCombineJob(Set<Integer> blockIndices) {
		MapCombineJob mcJob = new MapCombineJob(this, -1, blockIndices);
		mapCombineJobs.add(mcJob);
		jobAssignments.get(-1).add(mcJob);
		return mcJob;
	}

	public ReduceJob createReduceJob(int partitionNum, Set<Integer> mappers) {
		ReduceJob job = new ReduceJob(this, -1, partitionNum, mappers);
		reduceJobs.add(job);
		jobAssignments.get(-1).add(job);
		return job;
	}
	
	public ReduceCombineJob createReduceCombineJob() {
		return new ReduceCombineJob(id, -1, filename, getNumPartitions(), getPartitionReducers());
	}

	public synchronized boolean mapFinished(MapJob mapJob) {
		maxKey = Math.max(maxKey, mapJob.getMaxKey());
		minKey = Math.min(minKey, mapJob.getMinKey());
		mapJob.setDone(true);
		for (MapJob job : mapJobs) {
			if (!job.isDone()) {
				return false;
			}
		}
		return true;
	}

	public boolean mapCombineFinished(MapCombineJob job) {
		job.setDone(true);
		for (MapCombineJob mcjob : mapCombineJobs) {
			if (!mcjob.isDone()) {
				return false;
			}
		}
		return true;
	}
	
	public boolean reduceFinished(ReduceJob job) {
		job.setDone(true);
		for (ReduceJob reduce : reduceJobs) {
			if (!reduce.isDone()) {
				return false;
			}
		}
		return true;
	}
	
	public void reduceCombineFinished(ReduceCombineJob job) {
		job.setDone(true);
	}
	
	public Set<NodeJob> getAssignments(int nodeId) {
		return jobAssignments.get(nodeId);
	}
	
	public int getNumAssignments(int nodeId, Class<?> jobClazz) {
		int numAssignments = 0;
		Set<NodeJob> assignments = jobAssignments.get(nodeId);
		for (NodeJob job : assignments) {
			if (job.getClass().equals(jobClazz)) {
				numAssignments++;
			}
		}
		return numAssignments;
	}

	public MapJob getMapJob(int blockIndex) {
		for (MapJob mapJob : mapJobs) {
			if (mapJob.getBlockIndex() == blockIndex) {
				return mapJob;
			}
		}
		return null;
	}
	
	public int getWorker(NodeJob job) {
		for (int nodeId : jobAssignments.keySet()) {
			if (jobAssignments.get(nodeId).contains(job)) {
				return nodeId;
			}
		}
		return -1;
	}
	
	public Map<Integer, Set<Integer>> getNodeToBlocks() {
		Map<Integer, Set<Integer>> blockLocations = new HashMap<Integer, Set<Integer>>();
		for (int i = 0; i < numParticipants; i++) {
			Set<NodeJob> jobs = jobAssignments.get(i);
			Set<Integer> mappedBlocks = new HashSet<Integer>();
			for (NodeJob job : jobs) {
				if (job instanceof MapJob) {
					mappedBlocks.add(((MapJob) job).getBlockIndex());
				}
			}
			if (!mappedBlocks.isEmpty()) {
				blockLocations.put(i, mappedBlocks);
			}
		}
		return blockLocations;
	}

	public Class<?> getUserDefinedClass() {
		return clazz;
	}

	public int getId() {
		return id;
	}

	public String getFilename() {
		return filename;
	}

	public int getMaxKey() {
		return maxKey;
	}

	public int getMinKey() {
		return minKey;
	}

	public int getNumBlocks() {
		return numBlocks;
	}

	public int getNumPartitions() {
		return numParticipants;
	}

	@Override
	public String toString() {
		return String.format("id = %d, file = %s", id, filename);
	}
}