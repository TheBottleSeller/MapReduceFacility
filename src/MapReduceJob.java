import java.util.List;
import java.util.Map;

public class MapReduceJob {

	private int id;
	private String filename;
	private Class<?> clazz;
	private int numBlocks;
	private int[] mappers;
	private int[] reducers;
	private int completedMaps;
	private int completedCombines;
	private int completedReduces;
	private Map<Integer, List<NodeJob>> activeJobs;
	private int maxKey;
	private int minKey;

	public MapReduceJob(int id, Class<?> clazz, String filename, int numBlocks) {
		this.id = id;
		this.filename = filename;
		this.clazz = clazz;
		this.numBlocks = numBlocks;
		mappers = new int[numBlocks];
		reducers = new int[numBlocks];
		completedMaps = 0;
		completedCombines = 0;
		completedReduces = 0;
		maxKey = Integer.MIN_VALUE;
		minKey = Integer.MAX_VALUE;
	}
	
	public MapJob createMapJob(int blockIndex) {
		return new MapJob(id, filename, blockIndex, clazz);
	}
	
	public synchronized void addMapper(int nodeId, int blockIndex) {
		mappers[blockIndex] = nodeId;
	}

	public synchronized void addReducer(int nodeId, int partitionNo) {
		reducers[partitionNo] = nodeId;
	}
	
	public int[] getMappers() {
		return mappers;
	}
	
	public int getMapper(int blockIndex) {
		return mappers[blockIndex];
	}
	
	public int getReducer(int partitionNo) {
		return reducers[partitionNo];
	}
	
	public int[] getReducers() {
		return reducers;
	}
	
	public synchronized boolean mapFinished(MapJob mapJob) {
		completedMaps++;
		maxKey = Math.max(maxKey, mapJob.getMaxKey());
		minKey = Math.min(minKey, mapJob.getMinKey());
		return completedMaps == numBlocks;
	}

	public boolean combineFinished(int blocksCombined) {
		completedCombines += blocksCombined;
		return completedCombines == numBlocks;
	}

	public boolean reduceFinished() {
		completedReduces++;
		return completedReduces == numBlocks;
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
		return reducers.length;
	}

	@Override
	public String toString() {
		return String.format("id = %d, file = %s", id, filename);
	}

}