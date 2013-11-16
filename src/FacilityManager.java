import java.io.File;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface FacilityManager extends Remote {

	public Config getConfig() throws RemoteException;

	public boolean heartBeat() throws RemoteException;

	public int getNodeId() throws RemoteException;

	public void exit() throws RemoteException;

	public int dispatchJob(Class<?> clazz, String filename) throws RemoteException;
	
	public void runJob(NodeJob job) throws RemoteException;

	public void runMapJob(MapJob mapJob) throws RemoteException;

	public void runMapCombineJob(MapCombineJob mcJob) throws RemoteException;

	public void runReduceJob(ReduceJob reduceJob) throws RemoteException;

	public void runReduceCombineJob(ReduceCombineJob rcJob) throws RemoteException;
}