import java.util.List;

abstract class Mapper440<Kin,Vin,Kout,Vout> {
	
	public abstract List<KVPair<Kout,Vout>> map(KVPair<Kin,Vin> input);
	
}
