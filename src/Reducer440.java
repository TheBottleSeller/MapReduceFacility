import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
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

	private BufferedReader reader;
	private PrintWriter writer;

	public abstract KVPair<Kout, Vout> reduce(KVPair<String, List<Vin>> input);

	@Override
	public void run() {
		// gather files blocks while partition files are obtained
		partitionFiles = gatherFiles();

		File inPart = mergeSortPartitions(partitionFiles);

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
			readLines: while ((key = reader.readLine()) != null) {
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
			master.reduceFinished(jobId);
		} catch (IOException e) {
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
		String key = "";
		String minKey = null;
		for (File partition : partitionFiles) {
			
			BufferedReader reader = new BufferedReader(new FileReader(partition));
			readers.put(partition, reader);
			KVPairs<String, String> pairs = readKVPairs(reader);
			if (pairs == null) {
				partitionFiles.remove(partition);
				reader.close();
			}
			if (minKey == null) {
				minKey = key;
			}
			if (minKey.compareTo(key) == 1) {
				minKey = key;
			}
			
			currentKVPairs.put(partition, pairs);
		}
		
		SortedSet<File> lowestKeys = new TreeSet<File>(new Comparator<File>() {

			@Override
			public int compare(File partition1, File partition2) {
				KVPairs<String, String> pair1 = currentKVPairs.get(partition1);
				KVPairs<String, String> pair2 = currentKVPairs.get(partition2);
				return pair1.getKey().compareTo(pair2.getKey());
			}
			
		});
		
		KVPairs<String, String> currentMin = new KVPairs<String, String>(minKey, new ArrayList<String>());
		while (!lowestKeys.isEmpty()) {
			File lowestFile = lowestKeys.first();
			lowestKeys.remove(lowestFile);
			KVPairs<String, String> pair = currentKVPairs.get(lowestFile);
			if (pair.getKey().equals(currentMin.getKey())) {
				currentMin.addValues(pair.getValue());
				lowestKeys.add(lowestFile);
				lowes
			} else {
				mergedWriter.println()
			}
		}
		while (currentPair != null) {
			for (File partition : partitionFiles) {
				BufferedReader reader = readers.get(partition);
				KVPair<String, String> pair = currentKVPairs.get(partition);
				if (pair.getKey().compareTo(oai)
			}
		}
		// run merge sort
		return mergedFile;
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