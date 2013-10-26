import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class FSImpl implements FS {
	
	private Map<String, Map<Integer, Set<String>>> fsTable;

	public FSImpl() {
		fsTable = Collections.synchronizedMap(new HashMap<String, Map<Integer, Set<String>>>());
	}

	@Override
	public void upload(File localFile, String namespace) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public File getFile() {
		// TODO Auto-generated method stub
		return null;
	}
}
