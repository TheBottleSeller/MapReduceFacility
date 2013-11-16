import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;

public abstract class Mapper440 extends Thread {

	private FacilityManagerMaster master;
	private FS fs;
	private MapJob mapJob;
	private int nodeId;

	public abstract List<KVPair<String, String>> map(String record);

	@Override
	public void run() {
		String record;
		boolean success = false;
		try {
			// open user file block as input and output file
			File outFile = fs.makeMappedFileBlock(mapJob.getFilename(), mapJob.getBlockIndex(),
				mapJob.getId());
			File inFile = fs.getFileBlock(mapJob.getFilename(), mapJob.getBlockIndex());

			// open associated reader and writer
			BufferedReader reader = new BufferedReader(new FileReader(inFile));
			RecordWriter writer = new RecordWriter(outFile);

			while ((record = reader.readLine()) != null) {
				List<KVPair<String, String>> mappedRecord = map(record);
				for (KVPair<String, String> kvPair : mappedRecord) {
					int keyHash = kvPair.getKey().hashCode();
					mapJob.updateMaxMinKey(keyHash);
					writer.writeKeyValues(kvPair.getKey(), kvPair.getValue());
				}
			}
			reader.close();
			writer.close();
			success = true;
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			master.jobFinished(success, mapJob);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public void setMaster(FacilityManagerMaster master) {
		this.master = master;
	}

	public void setFS(FS fs) {
		this.fs = fs;
	}

	public void setMapJob(MapJob mapJob) {
		this.mapJob = mapJob;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
}