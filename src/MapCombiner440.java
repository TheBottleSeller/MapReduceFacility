import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class MapCombiner440 extends Thread {

	private FacilityManagerMaster master;
	private FS fs;
	private int jobId;
	private int nodeId;
	private String filename;
	private int numReducers;
	private Set<Integer> blockIndices;
	private Map<Integer, PrintWriter> partitionWriters;
	private Map<Integer, BufferedReader> blockReaders;
	private int minKey;
	private int maxKey;

	public MapCombiner440() {

	}

	public void init() throws IOException {
		partitionWriters = new HashMap<Integer, PrintWriter>(numReducers);
		for (int i = 0; i < numReducers; i++) {
			File partitionFile = fs.makePartitionFileBlock(filename, jobId, i);
			partitionWriters.put(i, new PrintWriter(new FileOutputStream(partitionFile)));
		}

		blockReaders = new HashMap<Integer, BufferedReader>(blockIndices.size());
		for (Integer blockIndex : blockIndices) {
			File blockFile = fs.getMappedFileBlock(filename, blockIndex, jobId);
			blockReaders.put(blockIndex, new BufferedReader(new FileReader(blockFile)));
		}
	}

	@Override
	public void run() {
		boolean success = true;
		BufferedReader blockReader;
		PrintWriter partitionWriter;
		for (Integer blockIndex : blockIndices) {
			blockReader = blockReaders.get(blockIndex);
			String key;
			String value;
			try {
				while ((key = blockReader.readLine()) != null) {
					value = blockReader.readLine();
					if (value == null) {
						continue;
					}
					int partitionNo = partition(key.hashCode());
					partitionWriter = partitionWriters.get(partitionNo);
					partitionWriter.println(key);
					partitionWriter.println(value);
				}
				blockReader.close();
			} catch (IOException e) {
				success = false;
			}
		}

		// Close all print writers
		for (PrintWriter writer : partitionWriters.values()) {
			writer.close();
		}

		// Read in each partition into memory, sort, and write back to file
		for (Integer partitionNo : partitionWriters.keySet()) {
			try {
				BufferedReader partitionReader = new BufferedReader(new FileReader(
					fs.getPartitionFileBlock(filename, jobId, partitionNo)));
				String key = null;
				String value = null;

				// Combine values for each key
				SortedMap<String, List<String>> data = new TreeMap<String, List<String>>();
				while ((key = partitionReader.readLine()) != null) {
					value = partitionReader.readLine();
					if (value == null) {
						continue;
					}
					List<String> values = data.get(key);
					if (values == null) {
						values = new ArrayList<String>();
						data.put(key, values);
					}
					values.add(value);
				}
				partitionReader.close();

				// Write aggregate records to disk
				partitionWriter = new PrintWriter(new FileOutputStream(fs.makePartitionFileBlock(
					filename, jobId, partitionNo)));
				for (String aggregateKey : data.keySet()) {
					List<String> values = data.get(aggregateKey);
					partitionWriter.println(aggregateKey);
					partitionWriter.println(values.size());
					for (String v : values) {
						partitionWriter.println(v);
					}
				}
				partitionWriter.close();

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// tell master that combine phase is done
		try {
			master.combineFinished(jobId, blockIndices.size());
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	private int partition(int key) {
		int range = maxKey - minKey / numReducers;
		int partitionNo = (key - minKey) / range;
		return partitionNo;
	}

	public void setMaster(FacilityManagerMaster master) {
		this.master = master;
	}

	public void setFs(FS fs) {
		this.fs = fs;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public void setNumReducers(int numReducers) {
		this.numReducers = numReducers;
	}

	public void setBlockIndices(Set<Integer> blockIndices) {
		this.blockIndices = blockIndices;
	}

	public void setPartitionWriters(Map<Integer, PrintWriter> partitionWriters) {
		this.partitionWriters = partitionWriters;
	}

	public void setBlockReaders(Map<Integer, BufferedReader> blockReaders) {
		this.blockReaders = blockReaders;
	}

	public void setMinKey(int minKey) {
		this.minKey = minKey;
	}

	public void setMaxKey(int maxKey) {
		this.maxKey = maxKey;
	}
}
