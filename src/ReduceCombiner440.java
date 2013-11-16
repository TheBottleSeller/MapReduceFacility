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

	private FacilityManagerMaster master;
	private FS fs;
	private ReduceCombineJob rcJob;

	public ReduceCombiner440() {

	}

	@Override
	public void run() {
		// Combine reduceFiles.
		File output = null;
		boolean success = false;
		try {
			output = fs.makeFinalOutputFile(rcJob.getFilename(), rcJob.getId());
			PrintWriter writer = new PrintWriter(new FileOutputStream(output));
			for (File reduceFile : gatherFiles()) {
				String line;
				BufferedReader reader = new BufferedReader(new FileReader(reduceFile));
				while ((line = reader.readLine()) != null) {
					writer.println(line);
				}
				reader.close();
			}
			success = true;
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			master.jobFinished(success, rcJob);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	private Set<File> gatherFiles() {
		int numPartitions = rcJob.getNumPartitions();
		final Set<File> reduceFiles = new HashSet<File>(numPartitions);

		for (int partitionNo = 0; partitionNo < numPartitions; partitionNo++) {
			final int pNo = partitionNo;
			Thread reductionRetriever = new Thread(new Runnable() {
				@Override
				public void run() {

					// blocks until the file is retrieved
					File reduceFile = fs.getFile(rcJob.getFilename(), rcJob.getId(),
						FS.FileType.REDUCER_OUT, pNo, rcJob.getReducer(pNo));
					reduceFiles.add(reduceFile);
				}
			});
			reductionRetriever.start();
		}

		while (reduceFiles.size() != numPartitions) {
			try {
				Thread.sleep(1000);
				// wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return reduceFiles;
	}

	public void setMaster(FacilityManagerMaster master) {
		this.master = master;
	}

	public void setFs(FS fs) {
		this.fs = fs;
	}

	public void setReduceCombineJob(ReduceCombineJob rcJob) {
		this.rcJob = rcJob;
	}
}
