import java.util.List;

public interface Container {
	
	public abstract class Mapper440<Kin,Vin,Kout,Vout> {
		
		public abstract List<KVPair<Kout,Vout>> map(KVPair<Kin,Vin> input);
	}
	
	public abstract class Reducer440<Kin, Vin, Kout, Vout> {
		
		public abstract KVPair<Kout, Vout> reduce(KVPair<Kin, List<Vin>> input);
	}
	
	public Mapper440<?, ?, ?, ?> getMapper();
	
	public Reducer440<?, ?, ?, ?> getReducer();
}