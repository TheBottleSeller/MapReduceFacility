import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class WordCountTest extends MapReduce440 {

	public class Mapper extends Mapper440<Integer, String, String, Integer> {

		public Mapper(FacilityManagerMaster master, File inBlock, File outBlock, int jobId,
			int nodeId, int blockIndex) {
			super(master, inBlock, outBlock, jobId, nodeId, blockIndex);
		}

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
	public Mapper440<?, ?, ?, ?> createMapper(FacilityManagerMaster master, File inBlock,
		File outBlock, int jobId, int nodeId, int blockIndex) {
		return new Mapper(master, inBlock, outBlock, jobId, nodeId, blockIndex);
	}

	public Reducer440<?, ?, ?, ?> createReducer() {
		return new Reducer();
	}
}
