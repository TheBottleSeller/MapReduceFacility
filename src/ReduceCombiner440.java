import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;

public class ReduceCombiner440 extends Thread {

	private FacilityManager manager;
	private FS fs;
	private int nodeId;
	private MapReduceJob job;

	public ReduceCombiner440() {

	}

	@Override
	public void run() {
		// Combine reduceFiles.
		File output = null;
		try {
			output = fs.makeFinalOutputFile(job.getFilename(), job.getId());
			PrintWriter writer = new PrintWriter(new FileOutputStream(output));
			for (File reduceFile : gatherFiles()) {
				String line;
				BufferedReader reader = new BufferedReader(new FileReader(reduceFile));
				while ((line = reader.readLine()) != null) {
					writer.write(line);
				}
				reader.close();
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			manager.outputFinished(output);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	private Set<File> gatherFiles() {
		int numPartitions = job.getNumPartitions();
		final Set<File> reduceFiles = new HashSet<File>(numPartitions);

		for (int partitionNo = 0; partitionNo < numPartitions; partitionNo++) {
			final int pNo = partitionNo;
			Thread reductionRetriever = new Thread(new Runnable() {
				@Override
				public void run() {

					// blocks until the file is retrieved
					File reduceFile = fs.getFile(job.getFilename(), job.getId(),
						FS.FileType.REDUCER_OUT, pNo, job.getReducer(pNo));
					reduceFiles.add(reduceFile);
					notifyAll();
				}
			});
			reductionRetriever.start();
		}

		while (reduceFiles.size() != numPartitions) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return reduceFiles;
	}

	public void setManager(FacilityManager manager) {
		this.manager = manager;
	}

	public void setFs(FS fs) {
		this.fs = fs;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
}
