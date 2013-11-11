import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
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

public abstract class Reducer440<Kin, Vin, Kout, Vout> extends Thread {

	private FacilityManagerMaster master;
	private FacilityManager manager;
	private FS fs;
	private ReduceJob job;
	private int nodeId;
	private Set<File> partitionFiles;

	public abstract KVPair<Kout, Vout> reduce(KVPair<String, List<Vin>> input);

	@Override
	public void run() {
		// gather files blocks while partition files are obtained
		partitionFiles = gatherFiles();

		File reduceInput = mergeSortPartitions(partitionFiles);
		
		boolean success = false;
		runReduce(reduceInput);
		
		master.reduceFinished(job.getJobId(), nodeId);
	}

	private Set<File> gatherFiles() {
		final Set<File> partitionFiles = new HashSet<File>(job.getMappers().size());

		for (final int mapperId : job.getMappers()) {
			Thread partitionRetriever = new Thread(new Runnable() {
				@Override
				public void run() {

					// blocks until the file is retrieved
					File partitionFile = fs.getFile(job.getFilename(), job.getJobId(),
						FS.FileType.PARTITION, job.getPartitionNum(), mapperId);
					partitionFiles.add(partitionFile);
					notifyAll();
				}
			});
			partitionRetriever.start();
		}

		while (partitionFiles.size() != job.getMappers().size()) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return partitionFiles;
	}

	private File mergeSortPartitions(Set<File> partitionFiles) throws IOException {
		// create merged partition file and writer
		File mergedFile = fs.makeReduceInputFile(job.getFilename(), job.getJobId(), job.getPartitionNum());
		PrintWriter mergedWriter = new PrintWriter(new FileOutputStream(mergedFile));
		
		// create partition readers
		Map<File, BufferedReader> readers = new HashMap<File, BufferedReader>(partitionFiles.size());
		final Map<File, KVPairs<String, String>> currentKVPairs = new HashMap<File, KVPairs<String, String>>(partitionFiles.size());
		String minKey = null;
		for (File partition : partitionFiles) {
			
			BufferedReader reader = new BufferedReader(new FileReader(partition));
			readers.put(partition, reader);
			KVPairs<String, String> pairs = readKVPairs(reader);
			if (pairs == null) {
				partitionFiles.remove(partition);
				reader.close();
				continue;
			}
			if (minKey == null) {
				minKey = pairs.getKey();
			}
			if (minKey.compareTo(pairs.getKey()) == 1) {
				minKey = pairs.getKey();
			}
			
			currentKVPairs.put(partition, pairs);
		}
		
		if (minKey == null) {
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
		
		KVPairs<String, String> currentMin = new KVPairs<String, String>(minKey, new ArrayList<String>());
		
		while (!lowestKeys.isEmpty()) {
			// pull out file with lowest current kv pair and remove from sorted set
			File lowestFile = lowestKeys.first();
			lowestKeys.remove(lowestFile);
			
			// get files current kvpair and reader
			KVPairs<String, String> pair = currentKVPairs.remove(lowestFile);
			BufferedReader reader = readers.get(lowestFile);
			
			// merge the kvpair and currentMin pair if keys are equal
			if (pair.getKey().equals(currentMin.getKey())) {
				currentMin.addValues(pair.getValue());
			} else {
				// print the currentMin key to merged file
				mergedWriter.println(currentMin.getKey());
				mergedWriter.println(currentMin.getValue().size());
				for (String v : currentMin.getValue()) {
					mergedWriter.println(v);
				}
				
				// set the currentMin to the previously read kvpair
				currentMin = pair;
			}
			
			// read next pair for lowestFile and add back to set if appropriate
			KVPairs<String, String> nextPair = readKVPairs(reader);
			if (nextPair != null) {
				currentKVPairs.put(lowestFile, nextPair);
				lowestKeys.add(lowestFile);
			} else {
				reader.close();
			}
		}
		
		// print the currentMin key to merged file
		mergedWriter.println(currentMin.getKey());
		mergedWriter.println(currentMin.getValue().size());
		for (String v : currentMin.getValue()) {
			mergedWriter.println(v);
		}
		
		mergedWriter.close();
		
		return mergedFile;
	}
	
	public void runReduce(File reduceInput) {
		File reducerOutput = fs.makeReduceOutputFile(job.getFilename(), job.getJobId(), job.getPartitionNum());
		PrintWriter writer  = new PrintWriter(new FileOutputStream(reducerOutput));
		BufferedReader reader = new BufferedReader(new FileReader(reduceInput));
		KVPairs<String, String> kvpairs;
		while ((kvpairs = readKVPairs(reader)) != null) {
			System.out.print("key = " + kvpairs.getKey() + ", values = " + Arrays.toString(kvpairs.getValue().toArray()));
			
			KVPair<Kout, Vout> reduction = reduce(kvpairs);
			
			writer.write(reduction.getKey() + "\n");
			writer.write(reduction.getValue() + "\n");
		}
		writer.close();
		reader.close();
	}
	
	
	public KVPairs<String, String> readKVPairs(BufferedReader reader) throws IOException {
		String key = reader.readLine();
		if (key == null) {
			return null;
		}
		KVPairs<String, String> pairs = new KVPairs<String, String>(key, new ArrayList<String>());
		int numValues = Integer.parseInt(reader.readLine());
		for (int i = 0; i < numValues; i++) {
			pairs.addValue(reader.readLine());
		}
		return pairs;
	}

	public void setMaster(FacilityManagerMaster master) {
		this.master = master;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public void setReduceJob(ReduceJob job) {
		this.job = job;
	}

	public void setFS(FS fs) {
		this.fs = fs;
	}

	public void setManager(FacilityManager manager) {
		this.manager = manager;
	}

}