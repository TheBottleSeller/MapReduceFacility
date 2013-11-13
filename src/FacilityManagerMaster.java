import java.io.FileNotFoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

public interface FacilityManagerMaster extends FacilityManager, Remote {

	public Map<Integer, Set<Integer>> distributeBlocks(String namespace, int numBlocks)
		throws RemoteException;

	public Config connect(int id) throws RemoteException, NotBoundException;

	public int redistributeBlock(int nodeId) throws RemoteException;

	public void updateFSTable(String namespace, int blockIndex, int nodeId) throws RemoteException;

	public boolean hasDistributedFile(String filename) throws RemoteException;

	public void mapFinished(boolean success, MapJob mapJob, int nodeId) throws RemoteException;

	public void combineFinished(boolean success, int jobId, int blocksCombined) throws RemoteException;

	public void reduceFinished(boolean success, int jobId) throws RemoteException, FileNotFoundException;

}
