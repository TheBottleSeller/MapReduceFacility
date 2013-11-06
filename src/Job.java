import java.util.HashMap;
import java.util.Map;

public class Job {

	private int id;
	private String filename;
	private Map<Integer, Integer> mappers;

	public Job(int id, String filename) {
		this.id = id;
		this.filename = filename;
		mappers = new HashMap<Integer, Integer>();
	}
	
	public void addMapper(int nodeId, int blockIndex) {
		mappers.put(nodeId, blockIndex);
	}
	
	public Map<Integer, Integer> getMappers() {
		return mappers;
	}
	
	public int getId() {
		return id;
	}
	
	@Override
	public String toString() {
		return String.format("id=%d file=%s", id, filename);
	}
}