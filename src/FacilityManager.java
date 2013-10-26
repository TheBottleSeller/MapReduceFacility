import java.util.Scanner;


public class FacilityManager extends Thread {
	private static final String PROMPT = "=> ";
	
	public void run() {
		Scanner scanner = new Scanner(System.in);
		System.out.print(PROMPT);

		while (scanner.hasNextLine()) {
			String command = scanner.nextLine();
		}
	}
	
}
