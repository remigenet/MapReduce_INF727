package MapReduce_INF727;

import java.io.IOException;
import java.util.concurrent.Callable;

public class map_shuffle_launcher implements Callable<String> {
	//callable class used for launching the map shuffle phase on the cluster
	//for information about this phase read the repport
	
	private final String machine;
	private final String number;
	private final String current_user;
    
    public map_shuffle_launcher(String m, String n, String cu){
        this.machine=m;
        this.number=n;
        this.current_user=cu;
    }
    
    @Override
    public String call() {
    	Process p;
        String[] launch_slave_cmd = {"ssh", "-o StrictHostKeyChecking=no", machine, "java","-jar", "/tmp/"+current_user+"/slave.jar","3", number,current_user};
    	ProcessBuilder pb_launch_slave = new ProcessBuilder(launch_slave_cmd);
    	pb_launch_slave.redirectErrorStream(true);
        try {
			p = pb_launch_slave.start();
			p.waitFor();
			p.destroy();
			return number+" 111";
		} catch (IOException e) {
			return number+" 000";
		} catch (InterruptedException e) {
			return number+" 000";
		}
	        

    }

}
