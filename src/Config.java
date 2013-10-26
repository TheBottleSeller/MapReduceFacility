import java.io.Serializable;

public class Config implements Serializable {
	
	private String[] participantsIp;
	private String masterIp;
	
	private int FSport;
	private int MRport;
	
	private int replicationFactor;
	private int fileBlockSize;
	
	private int maxMapsPerHost;
	private int maxReducesPerHost;
	
	

	public Config() {
		
	}
}
