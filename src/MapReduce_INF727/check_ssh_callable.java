package MapReduce_INF727;

import java.io.IOException;
import java.util.concurrent.Callable;

public class check_ssh_callable implements Callable<String> {
	//callable class to check if machine answer to ssh command and clean it 
	private String machine;
	private String current_user;
	
    public check_ssh_callable(String m, String cu){
        this.machine=m;
        this.current_user=cu;
    }
    
    @Override
    public String call() throws InterruptedException {
    	try {
			if(functions.ssh_working(machine,current_user)) {
				clean.clean_machine(machine,current_user);
				return machine;
			}
			else {
				return "000";
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			return "000";
		}
    	
    }

}
