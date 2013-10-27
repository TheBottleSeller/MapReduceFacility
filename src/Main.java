import java.io.File;


public class Main {

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Give config file name");
		}
		File cFile = new File(args[0]);
		Config config = new Config(cFile);
		
		FacilityManager manager = new FacilityManager();
		manager.run();
		
	}
}
