import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FSImpl implements FS {

	private Map<String, Map<Integer, Set<Integer>>> fsTable;
	private Config config;
	private int lastNode;

	public FSImpl(Config config) {
		this.fsTable = Collections
			.synchronizedMap(new HashMap<String, Map<Integer, Set<Integer>>>());
		this.config = config;
		this.lastNode = 0;
	}

	@Override
	public void upload(File file, String namespace) {
		int numLines = -1;
		try {
			numLines = getNumLines(file);
		} catch (IOException e) {
			e.printStackTrace();
		}

		int blockSize = config.getBlockSize();
		int numBlocks = numLines / blockSize + (numLines % blockSize == 0 ? 0 : 1);
		Map<Integer, Set<Integer>> blocksToNodes = Collections
			.synchronizedMap(new HashMap<Integer, Set<Integer>>());
		for (int i = 0; i < numBlocks; i++) {
			for (int j = 0; j < config.getReplicationFactor(); j++) {
				blocksToNodes.get(i).add(lastNode);
				blocksToNodes.put(i, blocksToNodes.get(i));
				lastNode = (lastNode + 1) % (config.getParticipantIps().length - 1);
			}
		}

		fsTable.put(namespace, blocksToNodes);
	}

	@Override
	public File getFile() {
		return null;
	}

	private int getNumLines(File file) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(file));
		try {
			byte[] c = new byte[1024];
			int count = 0;
			int readChars = 0;
			boolean empty = true;
			while ((readChars = is.read(c)) != -1) {
				empty = false;
				for (int i = 0; i < readChars; i++) {
					if (c[i] == '\n') {
						count++;
					}
				}
			}
			return (count == 0 && !empty) ? 1 : count;
		} finally {
			is.close();
		}
	}
}
