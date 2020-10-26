package MapReduce_INF727;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class master_map_and_shuffles_functions {

	@SuppressWarnings("unlikely-arg-type")
	public static machine_cluster gunzip_file(machine_cluster my_cluster, String split_folder, String jar_path, String current_user) throws InterruptedException, ExecutionException {
		//function that decompress the split file if needed
		//if a machine don't answer ask for a new deployment of the split on another and then decompress it
		
		ExecutorService executorService = Executors.newCachedThreadPool();
		ArrayList<String> todo_list = new ArrayList<String>();
		for (String split_number : my_cluster.machine_used.keySet()) {
			todo_list.add(split_number);
		}
		while(todo_list.size()>0) {
			Set<Callable<String>> callables = new HashSet<Callable<String>>();
			for (String split_number : todo_list) {
				callables.add(new gunzip_callable(my_cluster.machine_used.get(split_number), split_number, current_user));
			}
			List<Future<String>> futures = executorService.invokeAll(callables);
			callables.clear();
	        for (Future<String> future : futures) {
	            String result=future.get();
	            String split_number=result.split(" ")[0];
	            Integer worked=Integer.valueOf(result.split(" ")[1]);

	            if(worked.equals("000")==false) {
	            	todo_list=functions.delete_element(split_number, todo_list);
	            }
	            else {
	            	System.out.println("a machine fail during unziping, redeploying on another machine");
	            	my_cluster.machine_used.remove(split_number);
	            	String new_machine=my_cluster.machine_unused.get(0);
	            	my_cluster.machine_unused.remove(0);
	            	my_cluster.machine_used.put(split_number, new_machine);
	            	callables.add(new initial_deployer(new_machine,split_folder+"S"+split_number+".txt.gz", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split_number),current_user));
		        	
	            }
	        }
	
		}
		executorService.shutdown();
		System.out.println("finish unziping");
		return my_cluster;	
	}
	
	@SuppressWarnings("unlikely-arg-type")
	public static machine_cluster launch_map(machine_cluster my_cluster, String split_folder, String jar_path, boolean compression, String current_user) throws InterruptedException, ExecutionException {
		//launch the classic map phase of the project
		//if a machine don't answer ask for another deployement, and if needed decompression
		
		List<Future<String>> futures;
		boolean as_fail=false;
		ExecutorService executorService = Executors.newCachedThreadPool();
		ArrayList<String> todo_list = new ArrayList<String>();
		for (String split_number : my_cluster.machine_used.keySet()) {
			todo_list.add(split_number);
		}
		while(todo_list.size()>0) {
			Set<Callable<String>> callables = new HashSet<Callable<String>>();
			if(as_fail && compression) {
				for (String split_number : todo_list) {
					callables.add(new gunzip_callable(my_cluster.machine_used.get(split_number), split_number,current_user));
				}
				futures = executorService.invokeAll(callables);
			}
			as_fail=false;
			for (String split_number : todo_list) {
				callables.add(new map_launcher(my_cluster.machine_used.get(split_number), split_number,current_user));
			}
			futures = executorService.invokeAll(callables);
			callables.clear();
	        for (Future<String> future : futures) {
	            String result=future.get();
	            String split_number=result.split(" ")[0];
	            Integer worked=Integer.valueOf(result.split(" ")[1]);

	            if(worked.equals("000")==false) {
	            	todo_list=functions.delete_element(split_number, todo_list);
	            }
	            else {
	            	System.out.println("a machine fail during map, remaping on another machine");
	            	my_cluster.machine_used.remove(split_number);
	            	String new_machine=my_cluster.machine_unused.get(0);
	            	my_cluster.machine_unused.remove(0);
	            	my_cluster.machine_used.put(split_number, new_machine);
	            	if(compression) {
	            		callables.add(new initial_deployer(new_machine,split_folder+"S"+split_number+".txt.gz", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split_number),current_user));
	            	}else {	
	            		callables.add(new initial_deployer(new_machine,split_folder+"S"+split_number+".txt", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split_number),current_user));
	            	}
	            	as_fail=true;
	            }
	        }
	        if(as_fail) {futures = executorService.invokeAll(callables);}
	
		}
		executorService.shutdown();
		System.out.println("finish map");
		return my_cluster;
	}
	
	@SuppressWarnings("unlikely-arg-type")
	public static machine_cluster launch_map_shuffle(machine_cluster my_cluster, String split_folder, String jar_path, boolean compression, String current_user) throws InterruptedException, ExecutionException, FileNotFoundException, UnsupportedEncodingException {
		//launch the map shuffle on the cluster
		//if a machine don't work ask deployment on another and if needed decompression
		//then relaunch the map_shuffle on all machines
		
		boolean as_fail=false;
		List<Future<String>> futures;
		PrintWriter writer = new PrintWriter("/tmp/"+current_user+"/machines.txt", "UTF-8");
		ExecutorService executorService = Executors.newCachedThreadPool();
		ArrayList<String> todo_list = new ArrayList<String>();
		for(String split_number: my_cluster.machine_used.keySet()) {
			writer.println(split_number+" "+my_cluster.machine_used.get(split_number));
			todo_list.add(split_number);
		}
		ArrayList<String> todo_list_initial=todo_list;
		writer.close();
		Set<Callable<String>> callables = new HashSet<Callable<String>>();
		for (String split_number : todo_list) {
			callables.add(new deploy_file_callable(my_cluster.machine_used.get(split_number),"/tmp/"+current_user+"/machines.txt","/tmp/"+current_user+"/",split_number));
		}
		@SuppressWarnings("unused")
		List<Future<String>> futures_deploy = executorService.invokeAll(callables);
		callables.clear();
		while(todo_list.size()>0) {
			if(as_fail && compression) {
				for (String split_number : todo_list) {
					callables.add(new gunzip_callable(my_cluster.machine_used.get(split_number), split_number,current_user));
				}
				futures = executorService.invokeAll(callables);
				todo_list=todo_list_initial;
			}else if(as_fail) {
				todo_list=todo_list_initial;
			}
			as_fail=false;
			for (String split_number : todo_list) {
				callables.add(new map_shuffle_launcher(my_cluster.machine_used.get(split_number), split_number,current_user));
			}
			futures = executorService.invokeAll(callables);
			callables.clear();
	        for (Future<String> future : futures) {
	            String result=future.get();
	            String split_number=result.split(" ")[0];
	            Integer worked=Integer.valueOf(result.split(" ")[1]);

	            if(worked.equals("000")==false) {
	            	todo_list=functions.delete_element(split_number, todo_list);
	            }
	            else {
	            	System.out.println("a machine fail during map_shuffling, redeploying on a machine and relaunching");
	            	my_cluster.machine_used.remove(split_number);
	            	String new_machine=my_cluster.machine_unused.get(0);
	            	my_cluster.machine_unused.remove(0);
	            	my_cluster.machine_used.put(split_number, new_machine);
	            	if(compression) {
	            		callables.add(new initial_deployer(new_machine,split_folder+"S"+split_number+".txt.gz", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split_number),current_user));
	            	}else {	
	            		callables.add(new initial_deployer(new_machine,split_folder+"S"+split_number+".txt", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split_number),current_user));
	            	}
	            	as_fail=true;
	            }
	        }
	        @SuppressWarnings("unused")
			List<Future<String>> futures_init_deploy = executorService.invokeAll(callables);
	        callables.clear();
	
		}
		executorService.shutdown();
		System.out.println("finish map_shuffle");
		return my_cluster;
	}
	
	@SuppressWarnings("unlikely-arg-type")
	public static machine_cluster launch_map_reduce_shuffle(machine_cluster my_cluster, String split_folder, String jar_path, boolean compression, String current_user) throws InterruptedException, ExecutionException, FileNotFoundException, UnsupportedEncodingException {
		//launch the map reduce shuffle on the cluster
		//if a machine don't work ask deployment on another and if needed decompression
		//then relaunch the map_reduce_shuffle on all machines
				
		boolean as_fail=false;
		List<Future<String>> futures;
		PrintWriter writer = new PrintWriter("/tmp/"+current_user+"/machines.txt", "UTF-8");
		ExecutorService executorService = Executors.newCachedThreadPool();
		ArrayList<String> todo_list = new ArrayList<String>();
		for(String split_number: my_cluster.machine_used.keySet()) {
			writer.println(split_number+" "+my_cluster.machine_used.get(split_number));
			todo_list.add(split_number);
		}
		ArrayList<String> todo_list_initial=todo_list;
		writer.close();
		Set<Callable<String>> callables = new HashSet<Callable<String>>();
		for (String split_number : todo_list) {
			callables.add(new deploy_file_callable(my_cluster.machine_used.get(split_number),"/tmp/"+current_user+"/machines.txt","/tmp/"+current_user+"/",split_number));
		}
		@SuppressWarnings("unused")
		List<Future<String>> futures_deploy = executorService.invokeAll(callables);
		callables.clear();
		while(todo_list.size()>0) {
			if(as_fail && compression) {
				for (String split_number : todo_list) {
					callables.add(new gunzip_callable(my_cluster.machine_used.get(split_number), split_number,current_user));
				}
				futures = executorService.invokeAll(callables);
			}else if (as_fail) {
				todo_list=todo_list_initial;
			}
			as_fail=false;
			for (String split_number : todo_list) {
				callables.add(new map_reduce_shuffle_launcher(my_cluster.machine_used.get(split_number), split_number,current_user));
			}
			futures = executorService.invokeAll(callables);
			callables.clear();
	        for (Future<String> future : futures) {
	            String result=future.get();
	            String split_number=result.split(" ")[0];
	            Integer worked=Integer.valueOf(result.split(" ")[1]);

	            if(worked.equals("000")==false) {
	            	todo_list=functions.delete_element(split_number, todo_list);
	            }
	            else {
	            	System.out.println("a machine fail during map, remaping on another machine");
	            	my_cluster.machine_used.remove(split_number);
	            	String new_machine=my_cluster.machine_unused.get(0);
	            	my_cluster.machine_unused.remove(0);
	            	my_cluster.machine_used.put(split_number, new_machine);
	            	if(compression) {
	            		callables.add(new initial_deployer(new_machine,split_folder+"S"+split_number+".txt.gz", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split_number),current_user));
	            	}else {	
	            		callables.add(new initial_deployer(new_machine,split_folder+"S"+split_number+".txt", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split_number),current_user));
	            	}
	            	as_fail=true;
	            }
	        }
	        @SuppressWarnings("unused")
			List<Future<String>> futures_init_deploy = executorService.invokeAll(callables);
	        callables.clear();
	
		}
		executorService.shutdown();
		System.out.println("finish map reduce shuffle");
		return my_cluster;
	}
	
	@SuppressWarnings({ "unused", "unlikely-arg-type" })
	public static machine_cluster launch_shuffle(machine_cluster my_cluster, String split_folder, String jar_path, boolean compression, String current_user) throws FileNotFoundException, UnsupportedEncodingException, InterruptedException, ExecutionException {
		//launch the shuffle on the cluster
		//if a machine don't work ask deployment on another and if needed decompression, then the map
		//then relaunch the shuffle on all
				
		boolean as_fail=false;
		List<Future<String>> futures;
		PrintWriter writer = new PrintWriter("/tmp/"+current_user+"/machines.txt", "UTF-8");
		ExecutorService executorService = Executors.newCachedThreadPool();
		ArrayList<String> todo_list = new ArrayList<String>();
		for(String split_number: my_cluster.machine_used.keySet()) {
			writer.println(split_number+" "+my_cluster.machine_used.get(split_number));
			todo_list.add(split_number);
		}
		ArrayList<String> todo_list_initial=todo_list;
		writer.close();
		Set<Callable<String>> callables_shuffle = new HashSet<Callable<String>>();
		Set<Callable<String>> callables_initial_deploy = new HashSet<Callable<String>>();
		Set<Callable<String>> callables_gunzip = new HashSet<Callable<String>>();
		Set<Callable<String>> callables_map = new HashSet<Callable<String>>();
		int turn=0;
		while(todo_list.size()>0 || turn<=1) {

			Set<Callable<String>> callables = new HashSet<Callable<String>>();

			List<Future<String>> futures_deploy = executorService.invokeAll(callables_initial_deploy);
			List<Future<String>> futures_gunzip = executorService.invokeAll(callables_gunzip);
			List<Future<String>> futures_map = executorService.invokeAll(callables_map);
			for (String split_number : todo_list) {
				callables.add(new deploy_file_callable(my_cluster.machine_used.get(split_number),"/tmp/"+current_user+"/machines.txt","/tmp/"+current_user+"/",split_number));
			}

			futures = executorService.invokeAll(callables);
			List<Future<String>> futures_shuffles = executorService.invokeAll(callables_shuffle);
			callables.clear();
			callables_shuffle.clear();
			callables_map.clear();
			callables_initial_deploy.clear();
			if (turn==1) {

				futures=futures_shuffles;
			}
	        for (Future<String> future : futures) {
	        	
	            String result=future.get();
	            String split_number=result.split(" ")[0];
	            Integer worked=Integer.valueOf(result.split(" ")[1]);
	            if(worked.equals("000")==false) {
	            	todo_list=functions.delete_element(split_number, todo_list);
	            	callables_shuffle.add(new shuffle_launcher(my_cluster.machine_used.get(split_number),split_number,current_user));
	            }
	            else {
	            	System.out.println("a machine fail during shuffle, remaping on another machine and shuffle again");
	            	my_cluster.machine_used.remove(split_number);
	            	String new_machine=my_cluster.machine_unused.get(0);
	            	my_cluster.machine_unused.remove(0);
	            	my_cluster.machine_used.put(split_number, new_machine);
	            	if(compression) {
	            		callables_initial_deploy.add(new initial_deployer(new_machine,split_folder+"S"+split_number+".txt.gz", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split_number),current_user));
	            		callables.add(new gunzip_callable(my_cluster.machine_used.get(split_number), split_number,current_user));
	            	}else {	
	            		callables_initial_deploy.add(new initial_deployer(new_machine,split_folder+"S"+split_number+".txt", jar_path,"/tmp/"+current_user+"/splits/",Integer.valueOf(split_number),current_user));
	            	}
	            	as_fail=true;
	            	
	            	callables_map.add(new map_launcher(my_cluster.machine_used.get(split_number), split_number,current_user));
	            }
	        }
	        turn++;
		}
		
		executorService.shutdown();
		System.out.println("shuffle finished");
		return my_cluster;
	}	
}