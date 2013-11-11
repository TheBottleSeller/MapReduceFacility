import java.io.Serializable;


public class MapJob extends NodeJob implements Serializable {

	private static final long serialVersionUID = 166364L;

	private int blockIndex;
	private Class<?> clazz;
	
	public MapJob(int jobId, String filename, int blockIndex, Class<?> clazz) {
		super(jobId, filename);
		this.blockIndex = blockIndex;
		this.clazz = clazz;
	}
	
	public int getBlockIndex() {
		return blockIndex;
	}

	public Class<?> getClazz() {
		return clazz;
	}
}
