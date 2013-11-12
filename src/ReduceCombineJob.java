
public class ReduceCombineJob extends NodeJob {

	private static final long serialVersionUID = 15243672L;
	
	private int numPartitions;
	private int[] reducers;
	
	public ReduceCombineJob(MapReduceJob job) {
		super(job.getId(), job.getFilename());
		numPartitions = job.getNumPartitions();
		reducers = job.getReducers();
	}
	
	public int getNumPartitions() {
		return numPartitions;
	}
	
	public int getReducer(int partitionNum) {
		return reducers[partitionNum];
	}
	
}
