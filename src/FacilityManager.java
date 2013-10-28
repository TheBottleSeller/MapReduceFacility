import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

public interface FacilityManager extends Remote {
	
	Map<Integer, Set<Integer>> distributeBlocks() throws RemoteException; 
	
	public Config getConfig() throws RemoteException;

}
