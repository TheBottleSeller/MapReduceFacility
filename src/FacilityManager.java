import java.net.ServerSocket;
import java.util.Scanner;


public class FacilityManager extends Thread {
	private static final String PROMPT = "=> ";
	private ServerSocket socketServer;
	
	public void run() {
		Scanner scanner = new Scanner(System.in);
		System.out.print(PROMPT);

		while (scanner.hasNextLine()) {
			String command = scanner.nextLine();
		}
	}
	
	public void runServer() {
		
	}
	
}
