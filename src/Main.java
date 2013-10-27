import java.io.File;
import java.io.IOException;
import java.util.Map;


public class Main {

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("Give config file name");
		}
		File cFile = new File(args[0]);
		Config config = new Config(cFile);
		
		FacilityManager manager = new FacilityManager();
		manager.run();
		
		ProcessBuilder pb = new ProcessBuilder("myshellScript.sh", "myArg1", "myArg2");
		Map<String, String> env = pb.environment();
		env.put("VAR1", "myValue");
		env.remove("OTHERVAR");
		env.put("VAR2", env.get("VAR1") + "suffix");
		pb.directory(new File("myDir"));
		Process p = pb.start();
	}
}
