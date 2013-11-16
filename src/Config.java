import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;

public class Config implements Serializable {

	private static final long serialVersionUID = 1525640055670485006L;

	private String clusterName = null;

	private String masterIp = null;
	private String[] participantIps = null;

	private int fsReadPort = -1;
	private int fsWritePort = -1;
	private int mrPort = -1;

	private int maxMapsPerHost = -1;
	private int maxReducesPerHost = -1;

	private int replicationFactor = -1;
	private int blockSize = -1;

	public Config() {

	}

	public Config(File f) throws Exception {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (!(line.startsWith("#") || line.isEmpty())) {
					if (line.startsWith("CLUSTER_NAME")) {
						clusterName = getArgument(line);
					} else if (line.startsWith("MASTER_IP")) {
						masterIp = getArgument(line);
						InetAddress addr = InetAddress.getByName(masterIp);
						if (!InetAddress.getLocalHost().equals(addr)) {
							reader.close();
							throw new Exception("MASTER_IP must be equal to the local hostname.");
						}
					} else if (line.startsWith("PARTICIPANT_IPS")) {
						participantIps = getArgument(line).replaceAll("\\s+", "").split(",");
						boolean containsMasterIp = false;
						for (int i = 0; i < participantIps.length; i++) {
							if (participantIps[i].equals(masterIp)) {
								containsMasterIp = true;
								break;
							}
						}
						if (!containsMasterIp) {
							reader.close();
							throw new Exception("PARTICIPANT_IPS must contain MASTER_IP.");
						}
					} else if (line.startsWith("FS_READ_PORT")) {
						fsReadPort = Integer.parseInt(getArgument(line));
					} else if (line.startsWith("FS_WRITE_PORT")) {
						fsWritePort = Integer.parseInt(getArgument(line));
					} else if (line.startsWith("MR_PORT")) {
						mrPort = Integer.parseInt(getArgument(line));
					} else if (line.startsWith("MAX_MAPS_PER_HOST")) {
						maxMapsPerHost = Integer.parseInt(getArgument(line));
					} else if (line.startsWith("MAX_REDUCES_PER_HOST")) {
						maxReducesPerHost = Integer.parseInt(getArgument(line));
					} else if (line.startsWith("REPLICATION_FACTOR")) {
						replicationFactor = Integer.parseInt(getArgument(line));
					} else if (line.startsWith("BLOCK_SIZE")) {
						blockSize = Integer.parseInt(getArgument(line));
					}
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (isMissingArgument()) {
			throw new Exception("Config file is missing argument(s).");
		}
	}

	public boolean isMissingArgument() {
		return (clusterName == null || masterIp == null || participantIps == null
			|| fsReadPort == -1 || fsWritePort == -1 || mrPort == -1 || maxMapsPerHost == -1
			|| maxReducesPerHost == -1 || replicationFactor == -1 || blockSize == -1);
	}

	public String getArgument(String line) {
		return line.substring(line.indexOf('=') + 1).trim();
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
