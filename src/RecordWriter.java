import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.List;

public class RecordWriter {
	PrintWriter writer;

	public RecordWriter(File outFile) throws FileNotFoundException {
		this.writer = new PrintWriter(new FileOutputStream(outFile));
	}

	public void writeKeyValues(String key, String value) {
		writer.println(key);
		writer.println(value);
	}

	public void writeKeyMultiValues(String key, List<String> values) {
		writer.println(key);
		writer.println(values.size());
		for (String value : values) {
			writer.println(value);
		}
	}

	public void close() {
		if (writer != null) {
			writer.flush();
			writer.close();
		}
	}
}