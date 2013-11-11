import java.util.ArrayList;
import java.util.List;

public class WordCountTest extends MapReduce440 {

	public class Mapper extends Mapper440<Integer, String, String, Integer> {

		@Override
		public List<KVPair<String, Integer>> map(KVPair<Integer, String> input) {
			String line = input.getValue();
			String[] words = line.split(" ");
			List<KVPair<String, Integer>> tempValues = new ArrayList<KVPair<String, Integer>>();
			for (String word : words) {
				tempValues.add(new KVPair<String, Integer>(word, 1));
			}
			return tempValues;
		}
	}

	public class Reducer extends Reducer440<String, Integer, String, Integer> {

		@Override
		public KVPair<String, Integer> reduce(
				KVPair<String, List<String>> input) {
			String word = input.getKey();
			List<String> countString = input.getValue();
			List<Integer> counts = new ArrayList<Integer>(countString.size());
			for (String count : countString) {
				counts.add(Integer.parseInt(count));
			}
			int total = 0;
			for (int c : counts) {
				total += c;
			}
			return new KVPair<String, Integer>(word, total);
		}
	}

	@Override
	public Mapper440<?, ?, ?, ?> createMapper() {
		return new Mapper();
	}

	public Reducer440<?, ?, ?, ?> createReducer() {
		return new Reducer();
	}
}
