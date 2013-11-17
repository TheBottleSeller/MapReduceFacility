import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;

public class Config implements Serializable {

	private static final long serialVersionUID = 1525640055670485006L;

	private static final String[] ARGUMENTS = { "CLUSTER_NAME", "MASTER_IP", "PARTICIPANT_IPS",
		"FS_READ_PORT", "FS_WRITE_PORT", "MR_PORT", "MAX_MAPS_PER_HOST", "MAX_REDUCES_PER_HOST",
		"REPLICATION_FACTOR", "BLOCK_SIZE" };

	private String clusterName = null;

	private String masterIp = null;
	private String[] participantIps = null;

	private int fsReadPort;
	private int fsWritePort;
	private int rmiPort;

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
					if (line.startsWith(ARGUMENTS[0])) {
						clusterName = getArgument(line);
					} else if (line.startsWith(ARGUMENTS[1])) {
						masterIp = getArgument(line);
						InetAddress addr = InetAddress.getByName(masterIp);
						if (!InetAddress.getLocalHost().equals(addr)) {
							reader.close();
							throw new Exception("MASTER_IP must be equal to the local hostname.");
						}
					} else if (line.startsWith(ARGUMENTS[2])) {
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
					} else if (line.startsWith(ARGUMENTS[3])) {
						fsReadPort = Integer.parseInt(getArgument(line));
					} else if (line.startsWith(ARGUMENTS[4])) {
						fsWritePort = Integer.parseInt(getArgument(line));
					} else if (line.startsWith(ARGUMENTS[5])) {
						rmiPort = Integer.parseInt(getArgument(line));
					} else if (line.startsWith(ARGUMENTS[6])) {
						maxMapsPerHost = Integer.parseInt(getArgument(line));
					} else if (line.startsWith(ARGUMENTS[7])) {
						maxReducesPerHost = Integer.parseInt(getArgument(line));
					} else if (line.startsWith(ARGUMENTS[8])) {
						replicationFactor = Integer.parseInt(getArgument(line));
					} else if (line.startsWith(ARGUMENTS[9])) {
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

		if (hasBadArguments()) {
			throw new Exception("The given config file has invalid or missing argument(s).");
		}
	}

	public boolean hasBadArguments() {
		return (clusterName == null || masterIp == null || participantIps == null || fsReadPort < 1
			|| fsWritePort < 1 || rmiPort < 1 || maxMapsPerHost < 1 || maxReducesPerHost < 1
			|| replicationFactor < 1 || blockSize < 1);
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
		return rmiPort;
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
