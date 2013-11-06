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
	private File inBlock;
	private File outBlock;
	int jobId;
	int nodeId;
	int blockIndex;

	private BufferedReader reader;
	private PrintWriter writer;

	public Mapper440(FacilityManagerMaster master, File inBlock, File outBlock, int jobId,
		int nodeId, int blockIndex) {
		this.master = master;
		this.inBlock = inBlock;
		this.outBlock = outBlock;
		this.jobId = jobId;
		this.nodeId = nodeId;
		this.blockIndex = blockIndex;
	}

	public abstract List<KVPair<Kout, Vout>> map(KVPair<Integer, String> input);

	public void init() throws FileNotFoundException {
		reader = new BufferedReader(new FileReader(inBlock));
		writer = new PrintWriter(new FileOutputStream(outBlock));
	}

	@Override
	public void run() {
		int lineNum = 0;
		String line;
		try {
			while ((line = reader.readLine()) != null) {
				KVPair<Integer, String> record = new KVPair<Integer, String>(lineNum, line);
				List<KVPair<Kout, Vout>> mappedRecord = map(record);
				for (KVPair<Kout, Vout> kvPair : mappedRecord) {
					writer.write(kvPair.getKey() + "\n");
					writer.write(kvPair.getValue() + "\n");
				}
				lineNum++;
			}
			master.mapFinished(jobId, nodeId, blockIndex);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}