import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public interface FS {

	public void upload(File file, String namespace) throws IOException;

	public void mapreduce(InputStream is, String namespace) throws IOException;
	
	public File getFile();
}
