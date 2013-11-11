import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecordReader {
	BufferedReader reader;

	public RecordReader(File inFile) throws FileNotFoundException {
		this.reader = new BufferedReader(new FileReader(inFile));
	}

	public KVPair<String, String> readKeyValues() throws IOException {
		String key = reader.readLine();
		String value = reader.readLine();
		if (key == null || value == null) {
			return null;
		}
		return new KVPair<String, String>(key, value);
	}

	public KVPairs<String, String> readKeyMultiValues() throws IOException {
		String key = reader.readLine();
		String numValuesString = reader.readLine();
		if (key == null || numValuesString == null) {
			return null;
		}
		int numValues = Integer.parseInt(numValuesString);
		List<String> values = new ArrayList<String>(numValues);
		for (int i = 0; i < numValues; i++) {
			values.add(reader.readLine());
		}
		return new KVPairs<String, String>(key, values);
	}

}
