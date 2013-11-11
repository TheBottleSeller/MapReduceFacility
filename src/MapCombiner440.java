import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
	private Map<Integer, RecordWriter> partitionWriters;
	private Map<Integer, RecordReader> blockReaders;
	private int minKey;
	private int maxKey;

	public MapCombiner440() {

	}

	public void init() throws IOException {
		partitionWriters = new HashMap<Integer, RecordWriter>(numReducers);
		for (int i = 0; i < numReducers; i++) {
			File partitionFile = fs.makePartitionFileBlock(filename, jobId, i);
			partitionWriters.put(i, new RecordWriter(partitionFile));
		}

		blockReaders = new HashMap<Integer, RecordReader>(blockIndices.size());
		for (Integer blockIndex : blockIndices) {
			File blockFile = fs.getMappedFileBlock(filename, blockIndex, jobId);
			blockReaders.put(blockIndex, new RecordReader(blockFile));
		}
	}

	@Override
	public void run() {
		boolean success = true;
		RecordReader blockReader;
		RecordWriter partitionWriter;
		for (Integer blockIndex : blockIndices) {
			blockReader = blockReaders.get(blockIndex);
			KVPair<String, String> kvPair;
			try {
				while ((kvPair = blockReader.readKeyValue()) != null) {
					int partitionNo = partition(kvPair.getKey().hashCode());
					partitionWriter = partitionWriters.get(partitionNo);
					partitionWriter.writeKeyValues(kvPair.getKey(), kvPair.getValue());
				}
				blockReader.close();
			} catch (IOException e) {
				success = false;
			}
		}

		// Close all print writers
		for (RecordWriter writer : partitionWriters.values()) {
			writer.close();
		}

		// Read in each partition into memory, sort, and write back to file
		for (Integer partitionNo : partitionWriters.keySet()) {
			try {
				RecordReader partitionReader = new RecordReader(fs.getPartitionFileBlock(filename,
					jobId, partitionNo));
				KVPair<String, String> kvPair;

				// Combine values for each key
				SortedMap<String, List<String>> data = new TreeMap<String, List<String>>();
				while ((kvPair = partitionReader.readKeyValue()) != null) {
					List<String> values = data.get(kvPair.getKey());
					if (values == null) {
						values = new ArrayList<String>();
						data.put(kvPair.getKey(), values);
					}
					values.add(kvPair.getValue());
				}
				partitionReader.close();

				// Write aggregate records to disk
				partitionWriter = new RecordWriter(fs.makePartitionFileBlock(filename, jobId,
					partitionNo));
				for (String aggregateKey : data.keySet()) {
					List<String> values = data.get(aggregateKey);
					partitionWriter.writeKeyMultiValues(aggregateKey, values);
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

	public void setPartitionWriters(Map<Integer, RecordWriter> partitionWriters) {
		this.partitionWriters = partitionWriters;
	}

	public void setBlockReaders(Map<Integer, RecordReader> blockReaders) {
		this.blockReaders = blockReaders;
	}

	public void setMinKey(int minKey) {
		this.minKey = minKey;
	}

	public void setMaxKey(int maxKey) {
		this.maxKey = maxKey;
	}
}
