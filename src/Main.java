import java.io.File;


public class Main {

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Give config file name");
		}
		File config = new File(args[0]);
	}
	
	FileReader fr = new FileReader(config);
	

}
