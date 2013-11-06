import java.io.File;

public abstract class MapReduce440 {

	public abstract Mapper440<?, ?, ?, ?> createMapper(FacilityManagerMaster master, File inBlock,
		File outBlock, int jobId, int nodeId, int blockIndex);

	public abstract Reducer440<?, ?, ?, ?> createReducer();

}