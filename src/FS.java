import java.io.File;


public interface FS {
	
	public void upload(File localFile, String namespace);
	
	public File getFile();
	
}
