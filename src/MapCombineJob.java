
import java.util.Set;

public class MapCombineJob extends NodeJob {

	private static final long serialVersionUID = 52151L;
	
	private Set<Integer> blockIndices;
	private int maxKey;
	private int minKey;
	private int numPartitions;

	public MapCombineJob(MapReduceProgram job, int nodeId, Set<Integer> blockIndices) {
		super(job.getId(), nodeId, job.getFilename());
		this.blockIndices = blockIndices;
		maxKey = job.getMaxKey();
		minKey = job.getMinKey();
		numPartitions = job.getNumBlocks();
	}

	public Set<Integer> getBlockIndices() {
		return blockIndices;
	}

	public void setBlockIndices(Set<Integer> blockIndices) {
		this.blockIndices = blockIndices;
	}

	public int getMaxKey() {
		return maxKey;
	}

	public void setMaxKey(int maxKey) {
		this.maxKey = maxKey;
	}

	public int getMinKey() {
		return minKey;
	}

	public void setMinKey(int minKey) {
		this.minKey = minKey;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}
}
