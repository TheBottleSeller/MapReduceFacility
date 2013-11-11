import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public abstract class Mapper440 extends Thread {

	private FacilityManagerMaster master;
	private FS fs;
	private MapJob mapJob;
	private int nodeId;

	private BufferedReader reader;
	private RecordWriter writer;

	public void init() throws FileNotFoundException {
		reader = new BufferedReader(new FileReader(inBlock));
		writer = new RecordWriter(outBlock);
	}

	public abstract List<KVPair<String, String>> map(String record);

	@Override
	public void run() {
		String record;
		int maxKey = Integer.MIN_VALUE;
		int minKey = Integer.MAX_VALUE;
		try {
			while ((record = reader.readLine()) != null) {
				System.out.println("input record = " + record);
				List<KVPair<String, String>> mappedRecord = map(record);
				for (KVPair<String, String> kvPair : mappedRecord) {
					System.out.println("intermediate record = " + kvPair);
					int keyHash = kvPair.getKey().hashCode();
					maxKey = Math.max(maxKey, keyHash);
					minKey = Math.min(minKey, keyHash);
					writer.writeKeyValues(kvPair.getKey(), kvPair.getValue());
				}
			}
			writer.close();
			reader.close();
			master.mapFinished(jobId, nodeId, blockIndex, maxKey, minKey);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setMaster(FacilityManagerMaster master) {
		this.master = master;
	}

	public void setFS(FS fs) {

	}

	public void setInBlock(File inBlock) {
		this.inBlock = inBlock;
	}

	public void setOutBlock(File outBlock) {
		this.outBlock = outBlock;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public void setBlockIndex(int blockIndex) {
		this.blockIndex = blockIndex;
	}

	public void setReader(BufferedReader reader) {
		this.reader = reader;
	}

	public void setWriter(RecordWriter writer) {
		this.writer = writer;
	}
}