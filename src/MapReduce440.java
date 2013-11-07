import java.io.File;

public abstract class MapReduce440 {
	
	private Mapper440<?, ?, ?, ?> mapper;
	private Reducer440<?, ?, ?, ?> reducer;

	public abstract Reducer440<?, ?, ?, ?> createReducer();

	public Mapper440<?, ?, ?, ?> createMapper() {
		// TODO Auto-generated method stub
		return null;
	}

}