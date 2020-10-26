package MapReduce_INF727;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@SuppressWarnings("unused")
public class master_reduce {

    @SuppressWarnings("unlikely-arg-type")
    public static HashMap<String, Integer> launch_reduce(machine_cluster my_cluster, boolean first_exec, String current_user) throws IOException, InterruptedException, ExecutionException {
        //function that launch the reduce after map and shuffle
        //use callable class to launch everything and check success
        //handle machine trouble on cluster

        ProcessBuilder pb = new ProcessBuilder("hostname");
        pb.redirectErrorStream(true);
        ExecutorService executorService = Executors.newCachedThreadPool();
        Process p = pb.start();
        BufferedReader br2 = new BufferedReader(new InputStreamReader((p.getInputStream())));
        String name = br2.readLine();

        Set<Callable<String>> callables = new HashSet<>();
        Set<Callable<String>> callables_initial_deploy = new HashSet<>();
        for (String split_number : my_cluster.machine_used.keySet()) {
            callables.add(new reduce_launcher(my_cluster.machine_used.get(split_number), split_number, current_user + "@" + name, current_user));
        }
        List<Future<String>> futures = executorService.invokeAll(callables);
        boolean fail = false;
        for (Future<String> future : futures) {

            String result = future.get();
            String split_number = result.split(" ")[0];
            Integer worked = Integer.valueOf(result.split(" ")[1]);
            executorService.shutdown();
        }
        p.destroy();
        HashMap<String, Integer> reduce_result = new HashMap<>();
        if (first_exec) {
            String[] commande = {"ls", "/tmp/" + current_user + "/Reduce_received/"};
            ProcessBuilder pb2 = new ProcessBuilder(commande);
            pb2.redirectErrorStream(true);
            Process p2 = pb2.start();
            BufferedReader br = new BufferedReader(new InputStreamReader((p2.getInputStream())));
            String line;

            String word;
            String file;
            while ((file = br.readLine()) != null) {
                BufferedReader br3 = new BufferedReader(new FileReader("/tmp/" + current_user + "/Reduce_received/" + file));
                int count;
                while ((line = br3.readLine()) != null) {

                    ArrayList<String> result = new ArrayList<>(Arrays.asList(line.split(" {2}")));
                    word = result.get(0);
                    count = Integer.parseInt(result.get(result.size() - 1));

                    reduce_result.put(word, count);
                }
                br3.close();
            }
            System.out.println("reduce finished");
            p2.destroy();
        }
        reduce_result.remove(" ");
        reduce_result.remove("");
        return reduce_result;

    }

    @SuppressWarnings("unlikely-arg-type")
    public static HashMap<String, Integer> launch_reduce_for_map_reduce_shuflle(machine_cluster my_cluster, boolean first_exec, String current_user) throws IOException, InterruptedException, ExecutionException {
        //function that launch the reduce after map reduce shuffle
        //use callable class to launch everything and check success
        //handle machine trouble on cluster by redeploying on a new machine and relaunching the map reduce shuffle phase

        ProcessBuilder pb = new ProcessBuilder("hostname");
        pb.redirectErrorStream(true);
        ExecutorService executorService = Executors.newCachedThreadPool();
        Process p = pb.start();
        BufferedReader br2 = new BufferedReader(new InputStreamReader((p.getInputStream())));
        String name = br2.readLine();

        Set<Callable<String>> callables = new HashSet<>();
        Set<Callable<String>> callables_initial_deploy = new HashSet<>();
        for (String split_number : my_cluster.machine_used.keySet()) {
            callables.add(new reduce_for_map_reduce_shuffle_launcher(my_cluster.machine_used.get(split_number), split_number, current_user + "@" + name, current_user));
        }
        List<Future<String>> futures = executorService.invokeAll(callables);
        boolean fail = false;
        for (Future<String> future : futures) {

            String result = future.get();
            String split_number = result.split(" ")[0];
            Integer worked = Integer.valueOf(result.split(" ")[1]);
            executorService.shutdown();
        }
        p.destroy();
        HashMap<String, Integer> reduce_result = new HashMap<>();
        if (first_exec) {
            String[] commande = {"ls", "/tmp/" + current_user + "/Reduce_received/"};
            ProcessBuilder pb2 = new ProcessBuilder(commande);
            pb2.redirectErrorStream(true);
            Process p2 = pb2.start();
            BufferedReader br = new BufferedReader(new InputStreamReader((p2.getInputStream())));
            String line;

            String word;
            String file;
            while ((file = br.readLine()) != null) {

                BufferedReader br3 = new BufferedReader(new FileReader("/tmp/" + current_user + "/Reduce_received/" + file));
                int count;
                while ((line = br3.readLine()) != null) {

                    ArrayList<String> result = new ArrayList<>(Arrays.asList(line.split(" ")));
                    word = result.get(0);
                    count = Integer.parseInt(result.get(result.size() - 1));
                    reduce_result.put(word, count);

                }
                br3.close();
            }
            System.out.println("reduce finished");
            p2.destroy();
        }
        reduce_result.remove(" ");
        reduce_result.remove("");
        return reduce_result;

    }

}
