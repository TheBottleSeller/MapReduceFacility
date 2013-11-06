import java.util.ArrayList;
import java.util.List;

public class WordCountTest implements MapReduce440 {

	public class Mapper extends Mapper440<Integer, String, String, Integer> {

		@Override
		public List<KVPair<String, Integer>> map(KVPair<Integer, String> input) {
			String line = input.getValue();
			String[] words = line.split(" ");
			List<KVPair<String, Integer>> intermediateValues = new ArrayList<KVPair<String, Integer>>();
			for (String word : words) {
				intermediateValues.add(new KVPair<String, Integer>(word, 1));
			}
			return intermediateValues;
		}
	}

	public class Reducer extends Reducer440<String, Integer, String, Integer> {

		@Override
		public KVPair<String, Integer> reduce(KVPair<String, List<Integer>> input) {
			String word = input.getKey();
			List<Integer> counts = input.getValue();
			int total = 0;
			for (int c : counts) {
				total += c;
			}
			return new KVPair<String, Integer>(word, total);
		}

	}

	@Override
	public Mapper440<?, ?, ?, ?> getMapper() {
		return new Mapper();
	}

	@Override
	public Reducer440<?, ?, ?, ?> getReducer() {
		return new Reducer();
	}
}
