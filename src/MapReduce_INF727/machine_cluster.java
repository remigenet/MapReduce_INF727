package MapReduce_INF727;

import java.util.ArrayList;
import java.util.HashMap;

public class machine_cluster {
	//class used to store the cluster information
	//keep value as public in order to easily change them if any machine encounter a trouble
	public final HashMap<String, String> machine_used;
	public final ArrayList<String> machine_unused;
	
	public machine_cluster(HashMap<String, String> machine_used, ArrayList<String> machine_unused) {
		this.machine_used=machine_used;
		this.machine_unused=machine_unused;
	}
	
}
