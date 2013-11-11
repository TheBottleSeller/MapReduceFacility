import java.util.ArrayList;
import java.util.List;

public class WordCountTest extends MapReduce440 {

	public class Mapper extends Mapper440 {

		@Override
		public List<KVPair<String, String>> map(String record) {
			String[] words = record.split(" ");
			List<KVPair<String, String>> tempValues = new ArrayList<KVPair<String, String>>();
			for (String word : words) {
				tempValues.add(new KVPair<String, String>(word, 1 + ""));
			}
			return tempValues;
		}
	}

	public class Reducer extends Reducer440 {

		@Override
		public KVPair<String, String> reduce(KVPair<String, List<String>> input) {
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
			return new KVPair<String, String>(word, total + "");
		}
	}

	public Mapper440 createMapper() {
		return new Mapper();
	}

	public Reducer440 createReducer() {
		return new Reducer();
	}
}
