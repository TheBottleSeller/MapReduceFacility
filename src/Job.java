import java.util.HashMap;
import java.util.Map;

public class Job {

	private int id;
	private String filename;
	private int numBlocks;
	private int[] mappers;
	private int completedMaps;
	private int completedReduces;

	public Job(int id, String filename, int numBlocks) {
		this.id = id;
		this.filename = filename;
		this.numBlocks = numBlocks;
		mappers = new int[numBlocks];
		completedMaps = 0;
		completedReduces = 0;
	}
	
	public synchronized void addMapper(int nodeId, int blockIndex) {
		mappers[blockIndex] = nodeId;
	}
	
	public int getMapper(int blockIndex) {
		return mappers[blockIndex];
	}
	
	public synchronized boolean mapFinished() {
		completedMaps++;
		return completedMaps == numBlocks;
	}
	
	public int getId() {
		return id;
	}
	
	public int getNumBlocks() {
		return numBlocks;
	}
	
	@Override
	public String toString() {
		return String.format("id=%d file=%s", id, filename);
	}
}