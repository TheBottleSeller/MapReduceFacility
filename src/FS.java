import java.io.File;

public interface FS {

	public void upload(File file, String namespace);

	public File getFile();

}
