package MapReduce_INF727;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class master_cluster_and_deployement {


public static machine_cluster find_cluster(ArrayList<String> machines, int nb_machines, String split_folder, String jar_path, float backup_percent, String current_user) throws IOException, InterruptedException, ExecutionException {
	//function that check machine answer to a ssh connection and create a machine_cluster object with the one answering the more rapidly
	ExecutorService executorService = Executors.newCachedThreadPool();
	ArrayList<Integer> todo_list = new ArrayList<Integer>();
    ArrayList<String> machine_unused = new ArrayList<String>();
    HashMap<String, String> machine_used= new HashMap< String,String>();
    

    ProcessBuilder pb = new ProcessBuilder("hostname");
    pb.redirectErrorStream(true);
    Process p = pb.start();
    BufferedReader br2 = new BufferedReader(new InputStreamReader((p.getInputStream())));
    String master_node=br2.readLine();
    
    int machine_row=0;
    

	Set<Callable<String>> callables = new HashSet<Callable<String>>();
	
    for(int iter=0;iter<machines.size(); iter++) {
    	if (master_node.contentEquals(machines.get(machine_row))==false) {
    		callables.add(new initial_tester(current_user+"@"+machines.get(machine_row), current_user));
    	}
    	machine_row++;
    }
    List<Future<String>> futures = executorService.invokeAll(callables);
    Map<Integer, String> machine_speed=new TreeMap<Integer, String>();
    for (Future<String> future : futures) {
        String result=future.get();
        String machine=result.split(" ")[0];
        int time_taken=Integer.valueOf(result.split(" ")[2]);
        if(machine.equals("000")==false) {
        	machine_speed.put(time_taken, machine);
        }

    }
    int max_cluster_available= (int) ((float)(machine_speed.keySet().size())/(1.0+backup_percent));
    if(max_cluster_available<nb_machines) {
    	System.out.println("not enougth machine find, cluster size reduce to "+String.valueOf(max_cluster_available));
    }
    int my_cluster_size=Math.min(max_cluster_available, nb_machines);
    for(int iter=0;iter<my_cluster_size; iter++) {
    	todo_list.add(iter);
    }
    int count=0;
    for(Integer val: machine_speed.keySet()) {
    	if (count<my_cluster_size) {
    		machine_used.put(String.valueOf(todo_list.get(count)), machine_speed.get(val));
    	}
    	else {
    		machine_unused.add(machine_speed.get(val));
    	}
    	count++;
    }
    executorService.shutdown();
    machine_cluster my_cluster= new machine_cluster(machine_used, machine_unused);
    return my_cluster;
	
}

public static  machine_cluster deploy_split_on_cluster(machine_cluster my_cluster, String split_folder, String jar_path, boolean compression, String current_user) throws InterruptedException, ExecutionException {
	//deploy the split on the cluster
	//if a machine don't work choose another and send the split there
	
	boolean notalldone=true;
	ExecutorService executorService = Executors.newCachedThreadPool();
	while(notalldone) {
		notalldone=false;
	    Set<Callable<String>> callables_deploy = new HashSet<Callable<String>>();
	    for(String split: my_cluster.machine_used.keySet()) {
	    	if(compression) {
	    		callables_deploy.add(new initial_deployer(my_cluster.machine_used.get(split),split_folder+"S"+split+".txt.gz", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split), current_user));
	    	}else {
	    		callables_deploy.add(new initial_deployer(my_cluster.machine_used.get(split),split_folder+"S"+split+".txt", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split), current_user));
	    	}	    	
	    }
	    
	    List<Future<String>> futures_deploy = executorService.invokeAll(callables_deploy);
	    callables_deploy.clear();
	    for (Future<String> future : futures_deploy) {
            String result=future.get();
            String machine=result.split(" ")[0];
            if(machine.equals("000")==true) {
            	notalldone=true;
            	my_cluster.machine_used.remove(result.split(" ")[1]);
            	my_cluster.machine_used.put(result.split(" ")[0], my_cluster.machine_unused.get(0));
            	my_cluster.machine_unused.remove(0);
            	if(compression) {
            		callables_deploy.add(new initial_deployer(my_cluster.machine_used.get(result.split(" ")[1]),split_folder+"S"+result.split(" ")[1]+".txt.gz", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(result.split(" ")[1]), current_user));
            	    
            	}else {
            		callables_deploy.add(new initial_deployer(my_cluster.machine_used.get(result.split(" ")[1]),split_folder+"S"+result.split(" ")[1]+".txt", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(result.split(" ")[1]), current_user));
            	    
            	}
            	System.out.println("On machine down during deploy, deploying on backup machine");
            }
	    }
	}
	
	
    executorService.shutdown();
    return my_cluster;
}
	
}
