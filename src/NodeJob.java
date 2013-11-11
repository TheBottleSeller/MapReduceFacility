import java.io.Serializable;

public class NodeJob implements Serializable {

	private static final long serialVersionUID = 14324L;
	
	private int jobId;
	private String filename;
	
	public NodeJob(int jobId, String filename) {
		this.jobId = jobId;
		this.filename = filename;
	}

	public int getId() {
		return jobId;
	}

	public String getFilename() {
		return filename;
	}
}
