package MapReduce_INF727;

import java.io.IOException;
import java.util.concurrent.Callable;

public class deploy_file_callable implements Callable<String> {

	private String machine;
    private String data_file;
    private String directory;
    private String split_number;
	
    public deploy_file_callable(String machine, String data_file, String directory, String split_number){
        this.machine=machine;
        this.data_file=data_file;
        this.directory=directory;
        this.split_number=split_number;
    }
    
    @Override
    public String call() {
    	try {

			functions.check_or_create_dir(directory, machine);
			deploy.deploy_file(machine, data_file, directory);
			return split_number+" 111";

		} catch (IOException | InterruptedException e1) {
			e1.printStackTrace();
			return split_number+" 000";
		}
    	
    }

}