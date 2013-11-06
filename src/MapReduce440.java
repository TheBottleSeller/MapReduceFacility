import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public abstract class MapReduce440 {

	public class KVPair<K, V> {
		private K key;
		private V value;

		public KVPair(K key, V value) {
			this.key = key;
			this.value = value;
		}

		public K getKey() {
			return key;
		}

		public V getValue() {
			return value;
		}
	}

	public abstract class Mapper440<Kin, Vin, Kout, Vout> extends Thread {

		private File inBlock;
		private File outBlock;
		private BufferedReader reader;
		private PrintWriter writer;

		public Mapper440(File inBlock, File outBlock) {
			this.inBlock = inBlock;
			this.outBlock = outBlock;
		}

		public abstract List<KVPair<Kout, Vout>> map(KVPair<Integer, String> input);

		public void init() throws FileNotFoundException {
			reader = new BufferedReader(new FileReader(inBlock));
			writer = new PrintWriter(new FileOutputStream(outBlock));
		}

		@Override
		public void run() {
			int lineNum = 0;
			String line = "";
			try {
				while ((line = reader.readLine()) != null) {
					KVPair<Integer, String> record = new KVPair<Integer, String>(lineNum, line);
					List<KVPair<Kout, Vout>> mappedRecord = map(record);
					for (KVPair<Kout, Vout> kvPair : mappedRecord) {
						writer.write(kvPair.getKey() + "\n");
						writer.write(kvPair.getValue() + "\n");
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public abstract class Reducer440<Kin, Vin, Kout, Vout> {

		public abstract KVPair<Kout, Vout> reduce(KVPair<Kin, List<Vin>> input);

	}

	public abstract Mapper440<?, ?, ?, ?> createMapper(File inBlock, File outBlock);

	public abstract Reducer440<?, ?, ?, ?> createReducer();
}