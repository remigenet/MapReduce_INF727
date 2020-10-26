package MapReduce_INF727;

import java.io.IOException;
import java.util.concurrent.Callable;


public class initial_tester implements Callable<String> {
	// calable class used for testing if machine answer to ssh command and return time taken
	private String machine;
	private String current_user;
	
    public initial_tester(String machine, String cu){
        this.machine=machine;
        this.current_user=cu;
    }
    
    @Override
    public String call() {
    	try {
    		long time_connection;
			if((time_connection=functions.ssh_working_speed_test(machine, current_user))!=-1) {

				return machine+" 111 "+String.valueOf(time_connection);
				
			}
			else {
				return "000 000 0";
			}
		} catch (IOException | InterruptedException e1) {
			e1.printStackTrace();
			return "000 000 0";
		}
    	
    }
}
