import java.util.HashMap;
import java.util.Map;

public class Job {

	private int id;
	private String filename;
	private int numBlocks;
	private int[] mappers;
	private int completedMaps;
	private int completedReduces;
	private int maxKey;
	private int minKey;

	public Job(int id, String filename, int numBlocks) {
		this.id = id;
		this.filename = filename;
		this.numBlocks = numBlocks;
		mappers = new int[numBlocks];
		completedMaps = 0;
		completedReduces = 0;
		maxKey = Integer.MIN_VALUE;
		minKey = Integer.MAX_VALUE;
	}
	
	public synchronized void addMapper(int nodeId, int blockIndex) {
		mappers[blockIndex] = nodeId;
	}
	
	public int getMapper(int blockIndex) {
		return mappers[blockIndex];
	}
	
	public synchronized boolean mapFinished(int maxKey, int minKey) {
		completedMaps++;
		this.maxKey = Math.max(this.maxKey, maxKey);
		this.minKey = Math.min(this.minKey, minKey);
		return completedMaps == numBlocks;
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
	
	@Override
	public String toString() {
		return String.format("id=%d file=%s", id, filename);
	}
}