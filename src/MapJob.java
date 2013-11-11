import java.io.Serializable;


public class MapJob extends NodeJob implements Serializable {

	private static final long serialVersionUID = 166364L;

	private int blockIndex;
	private Class<?> clazz;
	private int maxKey;
	private int minKey;
	
	public MapJob(int jobId, String filename, int blockIndex, Class<?> clazz) {
		super(jobId, filename);
		this.blockIndex = blockIndex;
		this.clazz = clazz;
		maxKey = Integer.MIN_VALUE;
		minKey = Integer.MAX_VALUE;
	}
	
	public void updateMaxKey(int key) {
		maxKey = Math.max(maxKey, key);
	}
	
	public void updateMinKey(int key) {
		minKey = Math.min(minKey, key);
	}
	
	public int getBlockIndex() {
		return blockIndex;
	}

	public Class<?> getClazz() {
		return clazz;
	}
	
	public int getMaxKey() {
		return maxKey;
	}

	public int getMinKey() {
		return minKey;
	}
}
