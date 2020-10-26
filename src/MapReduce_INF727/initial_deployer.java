package MapReduce_INF727;

import java.io.IOException;
import java.util.concurrent.Callable;

public class initial_deployer implements Callable<String> {
	//class used on each of the cluster machine in order to clean it, and fill it with the file needed
	private final String machine;
    private final String data_file;
    private final String slave_file;
    private final String directory;
    private final Integer split_number;
    private final String current_user;
	
    public initial_deployer(String machine, String data_file, String slave_file, String directory, Integer split_number, String cu){
        this.machine=machine;
        this.data_file=data_file;
        this.slave_file=slave_file;
        this.directory=directory;
        this.split_number=split_number;
        this.current_user=cu;
    }
    
    @Override
    public String call() {
    	try {
			clean.clean_machine(machine, current_user);
			functions.check_or_create_dir(directory, machine);
			deploy.deploy_file(machine, data_file, directory);
			deploy.deploy_file(machine, slave_file, "/tmp/"+current_user+"/");
			return machine+" "+ split_number;
		} catch (IOException | InterruptedException e1) {
			e1.printStackTrace();
			return "000 "+ split_number;
		}
    	
    }

}
