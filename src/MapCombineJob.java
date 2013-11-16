import java.util.Arrays;
import java.util.Set;

public class MapCombineJob extends NodeJob {

	private static final long serialVersionUID = 52151L;

	private Set<Integer> blockIndices;
	private int maxKey;
	private int minKey;
	private int numPartitions;

	public MapCombineJob(MapReduceProgram prog, int nodeId, Set<Integer> blockIndices) {
		super(prog.getId(), prog.createNewJobId(), nodeId, prog.getFilename());
		this.blockIndices = blockIndices;
		maxKey = prog.getMaxKey();
		minKey = prog.getMinKey();
		numPartitions = prog.getNumPartitions();
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

	@Override
	public String toString() {
		return String.format("MapCombineJob %s blockIndices=%s numPartitions=%d maxKey=%d minKey=%d", super.toString(),
			Arrays.toString(blockIndices.toArray()), numPartitions, maxKey, minKey);
	}
}
