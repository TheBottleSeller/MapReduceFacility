import java.io.Serializable;
import java.util.Set;


public class ReduceJob extends NodeJob implements Serializable {
	
	private static final long serialVersionUID = 14552L;
	
	private Class<?> clazz;
	private int partitionNum;
	private Set<Integer> mappers;
	
	public ReduceJob(int jobId, String filename, int partitionNum, Set<Integer> mappers, Class<?> clazz) {
		super(jobId, filename);
		this.partitionNum = partitionNum;
		this.mappers = mappers;
		this.clazz = clazz;
	}

	public Class<?> getClazz() {
		return clazz;
	}

	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}

	public int getPartitionNum() {
		return partitionNum;
	}

	public void setPartitionNum(int partitionNum) {
		this.partitionNum = partitionNum;
	}

	public Set<Integer> getMappers() {
		return mappers;
	}

	public void setMappers(Set<Integer> mappers) {
		this.mappers = mappers;
	}	
}