import java.util.List;

public class KVPairs<K, V> extends KVPair<K, List<V>>{

	public KVPairs(K key, List<V> value) {
		super(key, value);
	}
	
	public void addValue(V value) {
		this.value.add(value);
	}
	
	public void addValues(List<V> value) {
		this.value.addAll(value);
	}
}
