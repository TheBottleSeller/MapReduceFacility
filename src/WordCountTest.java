import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class WordCountTest extends MapReduce440 {

	public class Mapper extends MapReduce440.Mapper440<Integer, String, String, Integer> {

		public Mapper(File inBlock, File outBlock) {
			super(inBlock, outBlock);
		}

		@Override
		public List<MapReduce440.KVPair<String, Integer>> map(
			MapReduce440.KVPair<Integer, String> input) {
			String line = input.getValue();
			String[] words = line.split(" ");
			List<MapReduce440.KVPair<String, Integer>> tempValues = new ArrayList<MapReduce440.KVPair<String, Integer>>();
			for (String word : words) {
				tempValues.add(new MapReduce440.KVPair<String, Integer>(word, 1));
			}
			return tempValues;
		}
	}

	public class Reducer extends MapReduce440.Reducer440<String, Integer, String, Integer> {

		@Override
		public MapReduce440.KVPair<String, Integer> reduce(
			MapReduce440.KVPair<String, List<Integer>> input) {
			String word = input.getKey();
			List<Integer> counts = input.getValue();
			int total = 0;
			for (int c : counts) {
				total += c;
			}
			return new MapReduce440.KVPair<String, Integer>(word, total);
		}

	}

	public MapReduce440.Mapper440<?, ?, ?, ?> createMapper(File inBlock, File outBlock) {
		return new Mapper(inBlock, outBlock);
	}

	public MapReduce440.Reducer440<?, ?, ?, ?> createReducer() {
		return new Reducer();
	}
}
