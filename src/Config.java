import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class Config implements Serializable {

	private static final long serialVersionUID = 1525640055670485006L;

	private String masterIp;
	private String[] participantIps;

	private int fsPort;
	private int mrPort;

	private int maxMapsPerHost;
	private int maxReducesPerHost;

	private int replicationFactor;
	private int blockSize;

	public Config() {

	}

	public Config(File f) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (!(line.startsWith("#") || line.isEmpty())) {
					if (line.startsWith("MASTER_IP")) {
						masterIp = line.substring(line.indexOf('=') + 1);
						System.out.println("masterIp: " + masterIp);
					} else if (line.startsWith("PARTICIPANT_IPS")) {
						participantIps = line.substring(line.indexOf('=') + 1).split(",");
						System.out.println("participantIps: " + Arrays.toString(participantIps));
					} else if (line.startsWith("FS_PORT")) {
						fsPort = Integer.parseInt(line.substring(line.indexOf('=') + 1));
						System.out.println("fsPort: " + fsPort);
					} else if (line.startsWith("MR_PORT")) {
						mrPort = Integer.parseInt(line.substring(line.indexOf('=') + 1));
						System.out.println("mrPort: " + mrPort);
					} else if (line.startsWith("MAX_MAPS_PER_HOST")) {
						maxMapsPerHost = Integer.parseInt(line.substring(line.indexOf('=') + 1));
						System.out.println("maxMapsPerHost: " + maxMapsPerHost);
					} else if (line.startsWith("MAX_REDUCES_PER_HOST")) {
						maxReducesPerHost = Integer.parseInt(line.substring(line.indexOf('=') + 1));
						System.out.println("maxReducesPerHost: " + maxReducesPerHost);
					} else if (line.startsWith("REPLICATION_FACTOR")) {
						replicationFactor = Integer.parseInt(line.substring(line.indexOf('=') + 1));
						System.out.println("replicationFactor: " + replicationFactor);
					} else if (line.startsWith("BLOCK_SIZE")) {
						blockSize = Integer.parseInt(line.substring(line.indexOf('=') + 1));
						System.out.println("blockSize: " + blockSize);
					}
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String getMasterIp() {
		return masterIp;
	}

	public String[] getParticipantIps() {
		return participantIps;
	}

	public int getFsPort() {
		return fsPort;
	}

	public int getMrPort() {
		return mrPort;
	}

	public int getMaxMapsPerHost() {
		return maxMapsPerHost;
	}

	public int getMaxReducesPerHost() {
		return maxReducesPerHost;
	}

	public int getReplicationFactor() {
		return replicationFactor;
	}

	public int getBlockSize() {
		return blockSize;
	}
}
