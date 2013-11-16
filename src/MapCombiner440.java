import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class MapCombiner440 extends Thread {

	private FacilityManagerMaster master;
	private FS fs;
	private MapCombineJob mcJob;

	public MapCombiner440() {

	}

	@Override
	public void run() {
		boolean success = false;
		try {
			// Create a RecordWriter for each partition
			Map<Integer, RecordWriter> partitionWriters = new HashMap<Integer, RecordWriter>(
				mcJob.getNumPartitions());
			for (int i = 0; i < mcJob.getNumPartitions(); i++) {
				File partitionFile = fs.makePartitionFileBlock(mcJob.getFilename(), mcJob.getId(),
					i);
				partitionWriters.put(i, new RecordWriter(partitionFile));
			}

			// Create a RecordReader for each block
			Map<Integer, RecordReader> blockReaders = new HashMap<Integer, RecordReader>(mcJob
				.getBlockIndices().size());
			for (Integer blockIndex : mcJob.getBlockIndices()) {
				File blockFile = fs.getMappedFileBlock(mcJob.getFilename(), blockIndex,
					mcJob.getId());
				blockReaders.put(blockIndex, new RecordReader(blockFile));
			}

			RecordReader blockReader;
			RecordWriter partitionWriter;
			for (Integer blockIndex : mcJob.getBlockIndices()) {
				blockReader = blockReaders.get(blockIndex);
				KVPair<String, String> kvPair;
				while ((kvPair = blockReader.readKeyValue()) != null) {
					int partitionNo = partition(kvPair.getKey().hashCode());
					partitionWriter = partitionWriters.get(partitionNo);
					partitionWriter.writeKeyValue(kvPair.getKey(), kvPair.getValue());
				}
				blockReader.close();
			}

			// Close all record writers
			for (RecordWriter writer : partitionWriters.values()) {
				writer.close();
			}

			// Read in each partition into memory, sort, and write back to file
			for (Integer partitionNo : partitionWriters.keySet()) {
				RecordReader partitionReader = new RecordReader(fs.getPartitionFileBlock(
					mcJob.getFilename(), mcJob.getId(), partitionNo));
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
				partitionWriter = new RecordWriter(fs.makePartitionFileBlock(mcJob.getFilename(),
					mcJob.getId(), partitionNo));
				for (String aggregateKey : data.keySet()) {
					List<String> values = data.get(aggregateKey);
					partitionWriter.writeKeyMultiValue(aggregateKey, values);
				}
				partitionWriter.close();
			}

			success = true;
		} catch (IOException e) {
			e.printStackTrace();
		}

		// tell master that combine phase is done
		try {
			master.jobFinished(success, mcJob);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	private int partition(int key) {
		int range = (int) (Math.ceil((mcJob.getMaxKey() - mcJob.getMinKey())
			/ (mcJob.getNumPartitions() * 1.0)));
		int partitionNo = (key - mcJob.getMinKey()) / range;
		return partitionNo;
	}

	public void setMaster(FacilityManagerMaster master) {
		this.master = master;
	}

	public void setFs(FS fs) {
		this.fs = fs;
	}

	public void setMapCombineJob(MapCombineJob mcJob) {
		this.mcJob = mcJob;
	}
}
