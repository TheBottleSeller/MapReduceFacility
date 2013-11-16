

public class ReduceCombineJob extends NodeJob {

	private static final long serialVersionUID = 15243672L;
	
	private int numPartitions;
	private int[] partitionReducers;
	
	public ReduceCombineJob(int jobId, int nodeId, String filename, int numPartitions, int[] partitionReducers) {
		super(jobId, nodeId, filename);
		this.numPartitions = numPartitions;
		this.partitionReducers = partitionReducers;
	}
	
	public int getNumPartitions() {
		return numPartitions;
	}
	
	public int getReducer(int partitionNum) {
		return partitionReducers[partitionNum];
	}
	
}
