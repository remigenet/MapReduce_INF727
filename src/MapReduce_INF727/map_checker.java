package MapReduce_INF727;

import java.io.IOException;
import java.util.concurrent.Callable;


public class map_checker implements Callable<String>{
	private final String machine;
	private final String number;
    
    public map_checker(String m, String n){
        this.machine=m;
        this.number=n;
    }
    
    @Override
    public String call() {
    	Process p;
        String[] launch_slave_cmd = {"ssh", "-o StrictHostKeyChecking=no", machine, "java","-jar", "/tmp/rgenet/slave.jar","0", number};
    	ProcessBuilder pb_launch_slave = new ProcessBuilder(launch_slave_cmd);
    	pb_launch_slave.redirectErrorStream(true);
        try {
			p = pb_launch_slave.start();
			p.waitFor();
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	        

    }


}

