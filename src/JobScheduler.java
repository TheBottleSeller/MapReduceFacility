import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class JobScheduler {
	
	private FacilityManagerMaster master;
	private Map<Integer, List<Job>> activeJobs;
	private int totalJobs;
	
	public JobScheduler(FacilityManagerMaster master) {
		this.master = master;
		activeJobs = Collections.synchronizedMap(new HashMap<Integer, List<Job>>());
		totalJobs = 0;
	}
	
	public Job issueJob(Class<?> clazz, Map<Integer, Set<Integer>> blockLocations) {
		try {
			MapReduce440 c = (MapReduce440) clazz.newInstance();
			Mapper440 mapper = c.getMapper();
			Reducer440 reducer = c.getReducer();
			System.out.println("mapper = " + mapper);
			System.out.println("reducer = " + reducer);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}
}
