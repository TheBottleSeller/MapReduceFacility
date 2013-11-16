import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MutualFriendsTest extends MapReduce440 {

	public class Mapper extends Mapper440 {

		@Override
		public List<KVPair<String, String>> map(String record) {
			String person = record.substring(0, record.indexOf(" "));
			String friends = record.substring(record.indexOf("[") + 1, record.indexOf("]"));
			List<KVPair<String, String>> tempValues = new ArrayList<KVPair<String, String>>();
			for (String friend : friends.split(",")) {
				if (person.hashCode() < friend.hashCode()) {
					tempValues.add(new KVPair<String, String>(String.format("[%s,%s]", person,
						friend), friends));
				} else {
					tempValues.add(new KVPair<String, String>(String.format("[%s,%s]", friend,
						person), friends));
				}
			}
			return tempValues;
		}
	}

	public class Reducer extends Reducer440 {

		@Override
		public KVPair<String, String> reduce(KVPair<String, List<String>> input) {
			String friends = input.getKey();
			List<String> friendLists = input.getValue();
			Set<String> mutualFriends = new HashSet<String>();
			for (int i = 0; i < friendLists.size(); i++) {
				List<String> friendList = Arrays.asList(friendLists.get(i).split(","));
				if (i == 0) {
					mutualFriends.addAll(friendList);
				} else {
					mutualFriends.retainAll(friendList);
				}
			}
			return new KVPair<String, String>(friends, Arrays.toString(mutualFriends.toArray()));
		}
	}

	public Mapper440 createMapper() {
		return new Mapper();
	}

	public Reducer440 createReducer() {
		return new Reducer();
	}
}
