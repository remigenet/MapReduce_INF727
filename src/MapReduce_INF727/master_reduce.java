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

public class master_reduce {

    @SuppressWarnings("unlikely-arg-type")
    public static HashMap<String, Integer> launch_reduce(machine_cluster my_cluster, String split_folder, String jar_path, boolean first_exec, boolean compression, String current_user) throws IOException, InterruptedException, ExecutionException {
        //function that launch the reduce after map and shuffle
        //use callable class to launch everything and check success
        //handle machine trouble on cluster

        ProcessBuilder pb = new ProcessBuilder("hostname");
        pb.redirectErrorStream(true);
        ExecutorService executorService = Executors.newCachedThreadPool();
        Process p = pb.start();
        BufferedReader br2 = new BufferedReader(new InputStreamReader((p.getInputStream())));
        String name = br2.readLine();

        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        Set<Callable<String>> callables_initial_deploy = new HashSet<Callable<String>>();
        for (String split_number : my_cluster.machine_used.keySet()) {
            callables.add(new reduce_launcher(my_cluster.machine_used.get(split_number), split_number, current_user + "@" + name, current_user));
        }
        List<Future<String>> futures = executorService.invokeAll(callables);
        boolean fail = false;
        for (Future<String> future : futures) {

            String result = future.get();
            String split_number = result.split(" ")[0];
            Integer worked = Integer.valueOf(result.split(" ")[1]);
            if (worked.equals("000")) {
                my_cluster.machine_used.remove(split_number);
                String new_machine = my_cluster.machine_unused.get(0);
                my_cluster.machine_unused.remove(0);
                my_cluster.machine_used.put(split_number, new_machine);
                if (compression) {
                    callables_initial_deploy.add(new initial_deployer(new_machine, split_folder + "S" + split_number + ".txt.gz", jar_path, "/tmp/" + current_user + "/splits/", Integer.valueOf(split_number), current_user));
                } else {
                    callables_initial_deploy.add(new initial_deployer(new_machine, split_folder + "S" + split_number + ".txt", jar_path, "/tmp/" + current_user + "/splits/", Integer.valueOf(split_number), current_user));
                }
                fail = true;
            }
            executorService.shutdown();
        }
        p.destroy();
        if (fail) {
            System.out.println("Fail detected during reduce, relaunching");
            System.out.println("launch map");
            System.out.println(my_cluster.machine_used);
            my_cluster = master_map_and_shuffles_functions.launch_map(my_cluster, "/tmp/" + current_user + "/splits/", jar_path, compression, current_user);
            System.out.println(my_cluster.machine_used);
            System.out.println("launch shuffle");
            my_cluster = master_map_and_shuffles_functions.launch_shuffle(my_cluster, "/tmp/" + current_user + "/splits/", jar_path, compression, current_user);
            System.out.println("launch reduce");
            clean.clean_here("/tmp/" + current_user + "/Reduce_received/");
            functions.check_or_create_dir("/tmp/" + current_user + "/Reduce_received/", null);
            @SuppressWarnings("unused")
            HashMap<String, Integer> reduce_result = launch_reduce(my_cluster, "/tmp/" + current_user + "/splits/", jar_path, false, compression, current_user);

        }
        HashMap<String, Integer> reduce_result = new HashMap<String, Integer>();
        if (first_exec) {
            String[] commande = {"ls", "/tmp/" + current_user + "/Reduce_received/"};
            ProcessBuilder pb2 = new ProcessBuilder(commande);
            pb2.redirectErrorStream(true);
            Process p2 = pb2.start();
            BufferedReader br = new BufferedReader(new InputStreamReader((p2.getInputStream())));
            String line;

            String word = null;
            String file;
            while ((file = br.readLine()) != null) {
                BufferedReader br3 = new BufferedReader(new FileReader("/tmp/" + current_user + "/Reduce_received/" + file));
                int count = 0;
                while ((line = br3.readLine()) != null) {

                    ArrayList<String> result = new ArrayList<>(Arrays.asList(line.split("  ")));
                    word = result.get(0);
                    count = Integer.valueOf(result.get(result.size() - 1));

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
    public static HashMap<String, Integer> launch_reduce_for_map_reduce_shuflle(machine_cluster my_cluster, String split_folder, String jar_path, boolean first_exec, boolean compression, String current_user) throws IOException, InterruptedException, ExecutionException {
        //function that launch the reduce after map reduce shuffle
        //use callable class to launch everything and check success
        //handle machine trouble on cluster by redeploying on a new machine and relaunching the map reduce shuffle phase

        ProcessBuilder pb = new ProcessBuilder("hostname");
        pb.redirectErrorStream(true);
        ExecutorService executorService = Executors.newCachedThreadPool();
        Process p = pb.start();
        BufferedReader br2 = new BufferedReader(new InputStreamReader((p.getInputStream())));
        String name = br2.readLine();

        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        Set<Callable<String>> callables_initial_deploy = new HashSet<Callable<String>>();
        for (String split_number : my_cluster.machine_used.keySet()) {
            callables.add(new reduce_for_map_reduce_shuffle_launcher(my_cluster.machine_used.get(split_number), split_number, current_user + "@" + name, current_user));
        }
        List<Future<String>> futures = executorService.invokeAll(callables);
        boolean fail = false;
        for (Future<String> future : futures) {

            String result = future.get();
            String split_number = result.split(" ")[0];
            Integer worked = Integer.valueOf(result.split(" ")[1]);
            if (worked.equals("000")) {
                my_cluster.machine_used.remove(split_number);
                String new_machine = my_cluster.machine_unused.get(0);
                my_cluster.machine_unused.remove(0);
                my_cluster.machine_used.put(split_number, new_machine);
                if (compression) {
                    callables_initial_deploy.add(new initial_deployer(new_machine, split_folder + "S" + split_number + ".txt.gz", jar_path, "/tmp/" + current_user + "/splits/", Integer.valueOf(split_number), current_user));
                } else {
                    callables_initial_deploy.add(new initial_deployer(new_machine, split_folder + "S" + split_number + ".txt", jar_path, "/tmp/" + current_user + "/splits/", Integer.valueOf(split_number), current_user));
                }
                fail = true;
            }
            executorService.shutdown();
        }
        p.destroy();
        if (fail) {
            System.out.println("Fail detected during reduce, relaunching");
            System.out.println("launch map");
            System.out.println(my_cluster.machine_used);
            my_cluster = master_map_and_shuffles_functions.launch_map_shuffle(my_cluster, "/tmp/" + current_user + "/splits/", jar_path, compression, current_user);
            System.out.println(my_cluster.machine_used);
            System.out.println("launch reduce");
            clean.clean_here("/tmp/" + current_user + "/Reduce_received/");
            functions.check_or_create_dir("/tmp/" + current_user + "/Reduce_received/", null);
            @SuppressWarnings("unused")
            HashMap<String, Integer> reduce_result = launch_reduce_for_map_reduce_shuflle(my_cluster, "/tmp/" + current_user + "/splits/", jar_path, false, compression, current_user);

        }
        HashMap<String, Integer> reduce_result = new HashMap<String, Integer>();
        if (first_exec) {
            String[] commande = {"ls", "/tmp/" + current_user + "/Reduce_received/"};
            ProcessBuilder pb2 = new ProcessBuilder(commande);
            pb2.redirectErrorStream(true);
            Process p2 = pb2.start();
            BufferedReader br = new BufferedReader(new InputStreamReader((p2.getInputStream())));
            String line;

            String word = null;
            String file;
            while ((file = br.readLine()) != null) {

                BufferedReader br3 = new BufferedReader(new FileReader("/tmp/" + current_user + "/Reduce_received/" + file));
                int count = 0;
                while ((line = br3.readLine()) != null) {

                    ArrayList<String> result = new ArrayList<>(Arrays.asList(line.split(" ")));
                    word = result.get(0);
                    count = Integer.valueOf(result.get(result.size() - 1));
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
