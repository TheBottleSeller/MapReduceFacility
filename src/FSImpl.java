import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FSImpl implements FS {

	private Map<String, Map<Integer, Set<Integer>>> fsTable;
	private Config config;
	private int lastNode;
	private FacilityManager master;

	public FSImpl(Config config, FacilityManager master) {
		this.fsTable = Collections
			.synchronizedMap(new HashMap<String, Map<Integer, Set<Integer>>>());
		this.config = config;
		this.master = master;
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
		
		try {
			master.distributeBlocks(namespace, numLines);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
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
