import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.List;

public abstract class Mapper440<Kin, Vin, Kout, Vout> extends Thread {

	private FacilityManagerMaster master;
	private FS fs;
	private MapJob mapJob;
	private int nodeId;

	public abstract List<KVPair<Kout, Vout>> map(KVPair<Integer, String> input);

	@Override
	public void run() {
		int lineNum = 0;
		String line;
		boolean success = false;
		try {
			// open user file block as input and output file
			File outFile = fs.makeMappedFileBlock(mapJob.getFilename(), mapJob.getBlockIndex(), mapJob.getId());
			File inFile = fs.getFileBlock(mapJob.getFilename(), mapJob.getBlockIndex());
			
			// open associated reader and writer
			BufferedReader reader = new BufferedReader(new FileReader(inFile));
			PrintWriter writer = new PrintWriter(new FileOutputStream(outFile));
			
			while ((line = reader.readLine()) != null) {
				KVPair<Integer, String> record = new KVPair<Integer, String>(lineNum, line);
				System.out.println("input record " + record);
				List<KVPair<Kout, Vout>> mappedRecord = map(record);
				for (KVPair<Kout, Vout> kvPair : mappedRecord) {
					System.out.println("intermediate record " + kvPair);
					int keyHash = kvPair.getKey().hashCode();
					mapJob.updateMaxKey(keyHash);
					mapJob.updateMinKey(keyHash);
					writer.println(kvPair.getKey());
					writer.println(kvPair.getValue());
				}
				lineNum++;
			}
			success = true;
			writer.close();
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			master.mapFinished(mapJob, nodeId);
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