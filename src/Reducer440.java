import java.util.List;

public abstract class Reducer440<Kin, Vin, Kout, Vout> {

	public abstract KVPair<Kout, Vout> reduce(KVPair<Kin, List<Vin>> input);

}