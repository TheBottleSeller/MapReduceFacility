import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public abstract class Mapper440<Kin, Vin, Kout, Vout> extends Thread {

	private FacilityManagerMaster master;
	private FS fs;
	private MapJob mapJob;
	private int nodeId;

	private BufferedReader reader;
	private PrintWriter writer;
	
	public void init() throws FileNotFoundException {
		reader = new BufferedReader(new FileReader(inBlock));
		writer = new PrintWriter(new FileOutputStream(outBlock));
	}

	public abstract List<KVPair<Kout, Vout>> map(KVPair<Integer, String> input);

	@Override
	public void run() {
		int lineNum = 0;
		String line;
		int maxKey = Integer.MIN_VALUE;
		int minKey = Integer.MAX_VALUE;
		try {
			while ((line = reader.readLine()) != null) {
				KVPair<Integer, String> record = new KVPair<Integer, String>(lineNum, line);
				System.out.println("input record " + record);
				List<KVPair<Kout, Vout>> mappedRecord = map(record);
				for (KVPair<Kout, Vout> kvPair : mappedRecord) {
					System.out.println("intermediate record " + kvPair);
					int keyHash = kvPair.getKey().hashCode();
					maxKey = Math.max(maxKey, keyHash);
					minKey = Math.min(minKey, keyHash);
					writer.write(kvPair.getKey() + "\n");
					writer.write(kvPair.getValue() + "\n");
				}
				lineNum++;
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

	public void setWriter(PrintWriter writer) {
		this.writer = writer;
	}
}