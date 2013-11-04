import java.io.File;
import java.io.IOException;

public interface FS {

	public void upload(File file, String namespace) throws IOException;

	public void mapreduce(Class<?> clazz, String namespace) throws IOException;
	
	public File getFile();
}
