public class KVPair<K, V> {
	private K key;
	protected V value;

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

	@Override
	public String toString() {
		return String.format("key = %s, value = %s", key, value);
	}
}