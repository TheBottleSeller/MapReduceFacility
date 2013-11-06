public class Job {

	public enum JobType {
		MAP, REDUCE
	};

	private int id;
	private JobType jobType;

	public Job(int id, JobType jobType) {
		this.id = id;
		this.jobType = jobType;
	}

	public JobType getJobType() {
		return jobType;
	}

	public int getId() {
		return id;
	}
	
	@Override
	public String toString() {
		return String.format("id=%d %s", id, jobType == JobType.MAP ? "MAP" : "REDUCE");
	}
}