import java.net.InetAddress;
import java.net.UnknownHostException;

public class Utils {

	public static String getLocalAddress() throws UnknownHostException {
		return InetAddress.getLocalHost().getHostAddress();
	}
}
