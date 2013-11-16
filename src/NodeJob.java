import java.io.Serializable;

public class NodeJob implements Serializable {

	private static final long serialVersionUID = 14324L;
	
	private int progId;
	private int jobId;
	private int nodeId;
	private String filename;
	private volatile boolean done;
	
	public NodeJob(int progId, int jobId, int nodeId, String filename) {
		this.progId = progId;
		this.jobId = jobId;
		this.nodeId = nodeId;
		this.filename = filename;
		this.done = false;
	}

	public int getId() {
		return progId;
	}
	
	public int getJobId() {
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
	
	@Override
	public String toString() {
		return String.format("progId=%d jobId=%d nodeId=%d", progId, jobId, nodeId);
	}
}
