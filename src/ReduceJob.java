import java.io.Serializable;
import java.util.Set;


public class ReduceJob implements Serializable {
	
	private int jobId;
	private String filename;
	private Class<?> clazz;
	private int partitionNum;
	private Set<Integer> mappers;
	
	public ReduceJob(int jobId, String filename, int partitionNum, Set<Integer> mappers, Class<?> clazz) {
		this.jobId = jobId;
		this.filename = filename;
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

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
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
