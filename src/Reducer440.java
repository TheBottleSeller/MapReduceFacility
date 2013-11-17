import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class Reducer440 extends Thread {

	private FacilityManagerMaster master;
	private FS fs;
	private ReduceJob job;
	private Set<File> partitionFiles;

	public abstract KVPair<String, String> reduce(KVPair<String, List<String>> input);

	@Override
	public void run() {
		boolean success = false;

		// gather files blocks while partition files are obtained
		partitionFiles = gatherFiles();
		try {
			// merge partition files
			File reduceInput = mergeSortPartitions(partitionFiles);

			// run reduce on merged file
			runReduce(reduceInput);

			success = true;
		} catch (IOException e) {
			e.printStackTrace();
		}

		// tell master that reduce job is finished
		try {
			master.jobFinished(success, job);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	private Set<File> gatherFiles() {
		final Set<File> partitionFiles = new HashSet<File>(job.getMappers().size());

		for (final int mapperId : job.getMappers()) {
			Thread partitionRetriever = new Thread(new Runnable() {
				@Override
				public void run() {
					// blocks until the file is retrieved
					File partitionFile = fs.getFile(job.getFilename(), job.getId(),
						FS.FileType.PARTITION, job.getPartitionNum(), mapperId);
					partitionFiles.add(partitionFile);
				}
			});
			partitionRetriever.start();
		}

		while (partitionFiles.size() != job.getMappers().size()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return partitionFiles;
	}

	private File mergeSortPartitions(Set<File> partitionFiles) throws IOException {
		// create merged partition file and writer
		File mergedFile = fs.makeReduceInputFile(job.getFilename(), job.getId(),
			job.getPartitionNum());
		RecordWriter mergedWriter = new RecordWriter(mergedFile);

		// create partition readers
		Map<File, RecordReader> readers = new HashMap<File, RecordReader>(partitionFiles.size());
		final Map<File, KVPairs<String, String>> currentKVPairs = new HashMap<File, KVPairs<String, String>>(
			partitionFiles.size());
		Set<File> emptyPartitions = new HashSet<File>();

		// run through each partition, opening RecordReader and keeping track of kvpair at top of
		// file
		for (File partition : partitionFiles) {

			RecordReader reader = new RecordReader(partition);
			readers.put(partition, reader);
			KVPairs<String, String> pairs = reader.readKeyMultiValues();
			if (pairs == null) {
				emptyPartitions.add(partition);
				reader.close();
				continue;
			}

			currentKVPairs.put(partition, pairs);
		}

		partitionFiles.removeAll(emptyPartitions);

		while (!partitionFiles.isEmpty()) {

			// find current min key
			String minKey = null;
			for (File partition : partitionFiles) {
				KVPairs<String, String> pair = currentKVPairs.get(partition);

				if (minKey == null || pair.getKey().compareTo(minKey) < 0) {
					minKey = pair.getKey();
				}
			}

			// merge all kvpairs at top of each partition with minKey
			KVPairs<String, String> minPair = new KVPairs<String, String>(minKey,
				new ArrayList<String>());
			emptyPartitions = new HashSet<File>();
			for (File partition : partitionFiles) {

				// get files current kvpair and reader
				KVPairs<String, String> pair = currentKVPairs.get(partition);

				if (pair.getKey().equals(minPair.getKey())) {
					minPair.addValues(pair.getValue());
					RecordReader reader = readers.get(partition);
					KVPairs<String, String> newPair = reader.readKeyMultiValues();
					if (newPair == null) {
						emptyPartitions.add(partition);
						reader.close();
						readers.remove(partition);
						currentKVPairs.remove(partition);
					} else {
						currentKVPairs.put(partition, newPair);
					}
				}
			}

			// print the currentMin key to merged file
			mergedWriter.writeKeyMultiValue(minPair.getKey(), minPair.getValue());

			partitionFiles.removeAll(emptyPartitions);
		}

		mergedWriter.close();
		return mergedFile;
	}

	// run reduce on merged partition file
	public void runReduce(File reduceInput) throws IOException {
		File reducerOutput = fs.makeReduceOutputFile(job.getFilename(), job.getId(),
			job.getPartitionNum());
		RecordWriter writer = new RecordWriter(reducerOutput);
		RecordReader reader = new RecordReader(reduceInput);
		KVPairs<String, String> kvPairs;
		while ((kvPairs = reader.readKeyMultiValues()) != null) {
			KVPair<String, String> reduction = reduce(kvPairs);
			writer.writeKeyValue(reduction.getKey(), reduction.getValue());
		}
		writer.close();
		reader.close();
	}

	public void setMaster(FacilityManagerMaster master) {
		this.master = master;
	}

	public void setReduceJob(ReduceJob job) {
		this.job = job;
	}

	public void setFS(FS fs) {
		this.fs = fs;
	}
}