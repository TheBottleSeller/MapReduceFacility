import java.io.Serializable;

public class NodeJob implements Serializable {

	private static final long serialVersionUID = 14324L;
	
	private int jobId;
	private int nodeId;
	private String filename;
	private volatile boolean done;
	
	public NodeJob(int jobId, int nodeId, String filename) {
		this.jobId = jobId;
		this.nodeId = nodeId;
		this.filename = filename;
		this.done = false;
	}

	public int getId() {
		return jobId;
	}
	
	public int getNodeId() {
		return nodeId;
	}

	public String getFilename() {
		return filename;
	}
	
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
	
	public void setDone(boolean done) {
		this.done = done;
	}
	
	public boolean isDone() {
		return done;
	}
}
