import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class Reducer440<Kin, Vin, Kout, Vout> extends Thread {

	private FacilityManagerMaster master;
	private FS fs;
	private File inPart;
	private File outPart;
	private int jobId;
	private int nodeId;
	private int numMappers;

	private BufferedReader reader;
	private PrintWriter writer;

	public void init() throws FileNotFoundException {
		reader = new BufferedReader(new FileReader(inPart));
		writer = new PrintWriter(new FileOutputStream(outPart));
	}

	public abstract KVPair<Kout, Vout> reduce(KVPair<String, List<Vin>> input);

	@Override
	public void run() {
		// wait for all partition files
		while (fs.getParititonFiles(jobId).size() != numMappers) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		Set<File> partitionFiles = fs.getParititonFiles(jobId);
		PrintWriter writer;
		try {
			writer = new PrintWriter(new FileOutputStream(inPart));
			
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		// TODO STOPPED HERE
		
		String line;
		String key;
		int numValues;
		List<Vin> values = new ArrayList<Vin>();
		try {
			readLines: 
				while ((key = reader.readLine()) != null) {
				line = reader.readLine();
				if (line == null) {
					continue;
				}

				numValues = Integer.parseInt(line);
				for (int i = 0; i < numValues; i++) {
					line = reader.readLine();
					if (line == null) {
						continue readLines;
					}
					values.add((Vin) reader.readLine());
				}

				System.out.print("key = " + key + ", values = " + values.toArray());

				KVPair<Kout, Vout> reduction = reduce(new KVPair<String, List<Vin>>(key, values));
				writer.write(reduction.getKey() + "\n");
				writer.write(reduction.getValue() + "\n");
			}
			writer.close();
			reader.close();
			master.reduceFinished(jobId, nodeId);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setMaster(FacilityManagerMaster master) {
		this.master = master;
	}

	public void setInBlock(File inBlock) {
		this.inPart = inBlock;
	}

	public void setOutBlock(File outBlock) {
		this.outPart = outBlock;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public void setReader(BufferedReader reader) {
		this.reader = reader;
	}

	public void setWriter(PrintWriter writer) {
		this.writer = writer;
	}
	
	public FS setFS(FS fs) {
		return fs;
	}

}