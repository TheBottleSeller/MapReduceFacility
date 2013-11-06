import java.util.List;

public interface MapReduce440 {

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

	public abstract class Mapper440<Kin, Vin, Kout, Vout> {

		public abstract List<KVPair<Kout, Vout>> map(KVPair<Kin, Vin> input);
		
	}

	public abstract class Reducer440<Kin, Vin, Kout, Vout> {

		public abstract KVPair<Kout, Vout> reduce(KVPair<Kin, List<Vin>> input);
		
	}

	public Mapper440<?, ?, ?, ?> getMapper();

	public Reducer440<?, ?, ?, ?> getReducer();
}