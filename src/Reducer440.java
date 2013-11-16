import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
					// notifyAll();
				}
			});
			partitionRetriever.start();
		}

		while (partitionFiles.size() != job.getMappers().size()) {
			try {
				Thread.sleep(1000);
				// wait();
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
		System.out.println("mergedFile = " + mergedFile.getAbsolutePath());
		RecordWriter mergedWriter = new RecordWriter(mergedFile);

		// create partition readers
		Map<File, RecordReader> readers = new HashMap<File, RecordReader>(partitionFiles.size());
		final Map<File, KVPairs<String, String>> currentKVPairs = new HashMap<File, KVPairs<String, String>>(
			partitionFiles.size());
		String minKey = null;
		Set<File> emptyPartitions = new HashSet<File>();
		for (File partition : partitionFiles) {

			RecordReader reader = new RecordReader(partition);
			readers.put(partition, reader);
			KVPairs<String, String> pairs = reader.readKeyMultiValues();
			if (pairs == null) {
				emptyPartitions.add(partition);
				reader.close();
				System.out.println("empty partition " + partition.getAbsolutePath());
				continue;
			}
			if (minKey == null || minKey.compareTo(pairs.getKey()) > 0) {
				minKey = pairs.getKey();
			}

			currentKVPairs.put(partition, pairs);
		}

		partitionFiles.removeAll(emptyPartitions);

		if (minKey == null) {
			mergedWriter.close();
			return mergedFile;
		}

		SortedSet<File> lowestKeys = new TreeSet<File>(new Comparator<File>() {

			@Override
			public int compare(File partition1, File partition2) {
				KVPairs<String, String> pair1 = currentKVPairs.get(partition1);
				KVPairs<String, String> pair2 = currentKVPairs.get(partition2);
				return pair1.getKey().compareTo(pair2.getKey());
			}

		});

		// add the files to the sorted set by current kv pair
		for (File partition : partitionFiles) {
			lowestKeys.add(partition);
		}

		KVPairs<String, String> currentMin = new KVPairs<String, String>(minKey,
			new ArrayList<String>());

		while (!lowestKeys.isEmpty()) {
			// pull out file with lowest current kv pair and remove from sorted set
			File lowestFile = lowestKeys.first();
			lowestKeys.remove(lowestFile);

			// get files current kvpair and reader
			KVPairs<String, String> pair = currentKVPairs.remove(lowestFile);
			RecordReader reader = readers.get(lowestFile);

			// merge the kvpair and currentMin pair if keys are equal
			if (pair.getKey().equals(currentMin.getKey())) {
				currentMin.addValues(pair.getValue());
			} else {
				// print the currentMin key to merged file
				mergedWriter.writeKeyMultiValue(currentMin.getKey(), currentMin.getValue());

				// set the currentMin to the previously read kvpair
				currentMin = pair;
			}

			// read next pair for lowestFile and add back to set if appropriate
			KVPairs<String, String> nextPair = reader.readKeyMultiValues();
			if (nextPair != null) {
				currentKVPairs.put(lowestFile, nextPair);
				lowestKeys.add(lowestFile);
			} else {
				reader.close();
			}
		}

		// print the currentMin key to merged file
		mergedWriter.writeKeyMultiValue(currentMin.getKey(), currentMin.getValue());
		mergedWriter.close();

		return mergedFile;
	}

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