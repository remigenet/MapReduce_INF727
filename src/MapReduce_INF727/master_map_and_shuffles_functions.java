package MapReduce_INF727;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@SuppressWarnings("ALL")
public class master_map_and_shuffles_functions {

	@SuppressWarnings("unlikely-arg-type")
	public static machine_cluster gunzip_file(machine_cluster my_cluster, String current_user) throws InterruptedException, ExecutionException {
		//function that decompress the split file if needed
		//if a machine don't answer ask for a new deployment of the split on another and then decompress it
		
		ExecutorService executorService = Executors.newCachedThreadPool();
		ArrayList<String> todo_list = new ArrayList<>(my_cluster.machine_used.keySet());
		while(todo_list.size()>0) {
			Set<Callable<String>> callables = new HashSet<>();
			for (String split_number : todo_list) {
				callables.add(new gunzip_callable(my_cluster.machine_used.get(split_number), split_number, current_user));
			}
			List<Future<String>> futures = executorService.invokeAll(callables);
			callables.clear();
	        for (Future<String> future : futures) {
	            String result=future.get();
	            String split_number=result.split(" ")[0];
	            Integer worked=Integer.valueOf(result.split(" ")[1]);

				functions.delete_element(split_number, todo_list);
			}
	
		}
		executorService.shutdown();
		System.out.println("finish unziping");
		return my_cluster;	
	}
	
	@SuppressWarnings("unlikely-arg-type")
	public static machine_cluster launch_map(machine_cluster my_cluster, String current_user) throws InterruptedException, ExecutionException {
		//launch the classic map phase of the project
		//if a machine don't answer ask for another deployement, and if needed decompression
		
		List<Future<String>> futures;
		boolean as_fail=false;
		ExecutorService executorService = Executors.newCachedThreadPool();
		ArrayList<String> todo_list = new ArrayList<>(my_cluster.machine_used.keySet());
		while(todo_list.size()>0) {
			Set<Callable<String>> callables = new HashSet<>();
			for (String split_number : todo_list) {
				callables.add(new map_launcher(my_cluster.machine_used.get(split_number), split_number,current_user));
			}
			futures = executorService.invokeAll(callables);
			callables.clear();
	        for (Future<String> future : futures) {
	            String result=future.get();
	            String split_number=result.split(" ")[0];
	            Integer worked=Integer.valueOf(result.split(" ")[1]);

				functions.delete_element(split_number, todo_list);
			}

		}
		executorService.shutdown();
		System.out.println("finish map");
		return my_cluster;
	}
	
	@SuppressWarnings("unlikely-arg-type")
	public static machine_cluster launch_map_shuffle(machine_cluster my_cluster, String current_user) throws InterruptedException, ExecutionException, IOException {
		//launch the map shuffle on the cluster
		//if a machine don't work ask deployment on another and if needed decompression
		//then relaunch the map_shuffle on all machines
		
		boolean as_fail=false;
		List<Future<String>> futures;
		PrintWriter writer = new PrintWriter("/tmp/"+current_user+"/machines.txt", StandardCharsets.UTF_8);
		ExecutorService executorService = Executors.newCachedThreadPool();
		ArrayList<String> todo_list = new ArrayList<>();
		for(String split_number: my_cluster.machine_used.keySet()) {
			writer.println(split_number+" "+my_cluster.machine_used.get(split_number));
			todo_list.add(split_number);
		}
		writer.close();
		Set<Callable<String>> callables = new HashSet<>();
		for (String split_number : todo_list) {
			callables.add(new deploy_file_callable(my_cluster.machine_used.get(split_number),"/tmp/"+current_user+"/machines.txt","/tmp/"+current_user+"/",split_number));
		}
		@SuppressWarnings("unused")
		List<Future<String>> futures_deploy = executorService.invokeAll(callables);
		callables.clear();
		while(todo_list.size()>0) {
			for (String split_number : todo_list) {
				callables.add(new map_shuffle_launcher(my_cluster.machine_used.get(split_number), split_number,current_user));
			}
			futures = executorService.invokeAll(callables);
			callables.clear();
	        for (Future<String> future : futures) {
	            String result=future.get();
	            String split_number=result.split(" ")[0];
	            Integer worked=Integer.valueOf(result.split(" ")[1]);

				functions.delete_element(split_number, todo_list);
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
	public static machine_cluster launch_map_reduce_shuffle(machine_cluster my_cluster, String current_user) throws InterruptedException, ExecutionException, IOException {
		//launch the map reduce shuffle on the cluster
		//if a machine don't work ask deployment on another and if needed decompression
		//then relaunch the map_reduce_shuffle on all machines
				
		boolean as_fail=false;
		List<Future<String>> futures;
		PrintWriter writer = new PrintWriter("/tmp/"+current_user+"/machines.txt", StandardCharsets.UTF_8);
		ExecutorService executorService = Executors.newCachedThreadPool();
		ArrayList<String> todo_list = new ArrayList<>();
		for(String split_number: my_cluster.machine_used.keySet()) {
			writer.println(split_number+" "+my_cluster.machine_used.get(split_number));
			todo_list.add(split_number);
		}
		writer.close();
		Set<Callable<String>> callables = new HashSet<>();
		for (String split_number : todo_list) {
			callables.add(new deploy_file_callable(my_cluster.machine_used.get(split_number),"/tmp/"+current_user+"/machines.txt","/tmp/"+current_user+"/",split_number));
		}
		@SuppressWarnings("unused")
		List<Future<String>> futures_deploy = executorService.invokeAll(callables);
		callables.clear();
		while(todo_list.size()>0) {
			for (String split_number : todo_list) {
				callables.add(new map_reduce_shuffle_launcher(my_cluster.machine_used.get(split_number), split_number,current_user));
			}
			futures = executorService.invokeAll(callables);
			callables.clear();
	        for (Future<String> future : futures) {
	            String result=future.get();
	            String split_number=result.split(" ")[0];
	            Integer worked=Integer.valueOf(result.split(" ")[1]);

				functions.delete_element(split_number, todo_list);
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
	public static machine_cluster launch_shuffle(machine_cluster my_cluster, String split_folder, String jar_path, boolean compression, String current_user) throws IOException, InterruptedException, ExecutionException {
		//launch the shuffle on the cluster
		//if a machine don't work ask deployment on another and if needed decompression, then the map
		//then relaunch the shuffle on all
				
		boolean as_fail=false;
		List<Future<String>> futures;
		PrintWriter writer = new PrintWriter("/tmp/"+current_user+"/machines.txt", StandardCharsets.UTF_8);
		ExecutorService executorService = Executors.newCachedThreadPool();
		ArrayList<String> todo_list = new ArrayList<>();
		for(String split_number: my_cluster.machine_used.keySet()) {
			writer.println(split_number+" "+my_cluster.machine_used.get(split_number));
			todo_list.add(split_number);
		}
		writer.close();
		Set<Callable<String>> callables_shuffle = new HashSet<>();
		Set<Callable<String>> callables_initial_deploy = new HashSet<>();
		Set<Callable<String>> callables_gunzip = new HashSet<>();
		Set<Callable<String>> callables_map = new HashSet<>();
		int turn=0;
		while(todo_list.size()>0 || turn<=1) {

			Set<Callable<String>> callables = new HashSet<>();

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
				functions.delete_element(split_number, todo_list);
				callables_shuffle.add(new shuffle_launcher(my_cluster.machine_used.get(split_number),split_number,current_user));
			}
	        turn++;
		}
		
		executorService.shutdown();
		System.out.println("shuffle finished");
		return my_cluster;
	}	
}
