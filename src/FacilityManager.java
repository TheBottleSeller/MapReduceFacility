import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Set;

public interface FacilityManager extends Remote {

	public Config getConfig() throws RemoteException;

	public boolean heartBeat() throws RemoteException;

	public int getNodeId() throws RemoteException;

	public void exit() throws RemoteException;

	public int dispatchJob(Class<?> clazz, String filename)
			throws RemoteException;

	public boolean runMapJob(int jobId, String filename, int blockIndex,
			Class<?> clazz) throws RemoteException;

	public boolean runCombineJob(Set<Integer> blockIndices, String namespace,
			int jobId, int maxKey, int minKey, int numReducers)
			throws RemoteException;

	public boolean runReduceJob(int jobId, Set<Integer> mapperIds,
			int partitionNo) throws RemoteException;
}