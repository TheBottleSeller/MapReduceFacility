import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;

public class Config implements Serializable {

	private static final long serialVersionUID = 1525640055670485006L;

	private String clusterName;

	private String masterIp;
	private String[] participantIps;

	private int fsReadPort;
	private int fsWritePort;
	private int mrPort;

	private int maxMapsPerHost;
	private int maxReducesPerHost;

	private int replicationFactor;
	private int blockSize;

	public Config() {

	}

	public Config(File f) throws Exception {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (!(line.startsWith("#") || line.isEmpty())) {
					if (line.startsWith("CLUSTER_NAME")) {
						clusterName = line.substring(line.indexOf('=') + 1);
						System.out.println("clustername = " + clusterName);
					} else if (line.startsWith("MASTER_IP")) {
						masterIp = line.substring(line.indexOf('=') + 1);
						InetAddress addr = InetAddress.getByName(masterIp);
						if (!InetAddress.getLocalHost().equals(addr)) {
							throw new Exception("MASTER_IP must be equal to the local hostname.");
						}
					} else if (line.startsWith("PARTICIPANT_IPS")) {
						participantIps = line.substring(line.indexOf('=') + 1).split(",");
						boolean containsMasterIp = false;
						for (int i = 0; i < participantIps.length; i++) {
							if (participantIps[i].equals(masterIp)) {
								containsMasterIp = true;
								break;
							}
						}
						if (!containsMasterIp) {
							throw new Exception("PARTICIPANT_IPS must contain MASTER_IP.");
						}
					} else if (line.startsWith("FS_READ_PORT")) {
						fsReadPort = Integer.parseInt(line.substring(line.indexOf('=') + 1));
					} else if (line.startsWith("FS_WRITE_PORT")) {
						fsWritePort = Integer.parseInt(line.substring(line.indexOf('=') + 1));
					} else if (line.startsWith("MR_PORT")) {
						mrPort = Integer.parseInt(line.substring(line.indexOf('=') + 1));
					} else if (line.startsWith("MAX_MAPS_PER_HOST")) {
						maxMapsPerHost = Integer.parseInt(line.substring(line.indexOf('=') + 1));
					} else if (line.startsWith("MAX_REDUCES_PER_HOST")) {
						maxReducesPerHost = Integer.parseInt(line.substring(line.indexOf('=') + 1));
					} else if (line.startsWith("REPLICATION_FACTOR")) {
						replicationFactor = Integer.parseInt(line.substring(line.indexOf('=') + 1));
					} else if (line.startsWith("BLOCK_SIZE")) {
						blockSize = Integer.parseInt(line.substring(line.indexOf('=') + 1));
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

	public String getClusterName() {
		return clusterName;
	}

	public String getMasterIp() {
		return masterIp;
	}

	public String[] getParticipantIps() {
		return participantIps;
	}

	public String getNodeAddress(int nodeId) {
		return participantIps[nodeId];
	}

	public int getFsReadPort() {
		return fsReadPort;
	}

	public int getFsWritePort() {
		return fsWritePort;
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
