import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

public interface FacilityManager extends Remote {
	
	public Map<Integer, Set<Integer>> distributeBlocks(String namespace, int numRecords) throws RemoteException; 
	
	public Config getConfig() throws RemoteException;
	
	public Config connect(int id) throws RemoteException, NotBoundException;
	
	public boolean heartBeat() throws RemoteException;

	public int getNodeId() throws RemoteException;

	public int redistributeBlock(int nodeId) throws RemoteException;

	public void updateFSTable(String namespace, int blockIndex, int nodeId) throws RemoteException;
}