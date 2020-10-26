package MapReduce_INF727;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class slave {
    //slave class that is intended to be export as jar runnable
    //this class is then deploy on machine of the cluster and execute map/shuffle/reduce phase on them

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        //launch selected functions depending of inputs
        if (args[0].contentEquals("-1")) {
            gunzip_split(args[1], args[2]);
        }
        if (args[0].equals("0")) {
            map(args[1], args[2]);
        } else if (args[0].equals("1")) {
            shuffle(args[1], args[2]);
        } else if (args[0].equals("2")) {
            reduce(args[1], args[3], args[2]);
        } else if (args[0].equals("3")) {
            map_shuffle(args[1], args[2]);
        } else if (args[0].equals("4")) {
            map_reduce_shuffle(args[1], args[2]);
        } else if (args[0].equals("5")) {
            reduce_for_map_reduce_shuffle(args[1], args[3], args[2]);
        }

    }

    public static void map(String split_number, String current_user) throws IOException, InterruptedException {
        //classic map phase as asked on the project
        //read the file and write word on separate lines
        String line;
        //create directory where the files will be put
        functions.check_or_create_dir("/tmp/" + current_user + "/maps/", null);
        functions.check_or_create_dir("/tmp/" + current_user + "/shuffles_received/", null);
        //open reader and writer, used buffered one for performance
        BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/" + current_user + "/maps/UM" + split_number + ".txt", true));
        BufferedReader br = new BufferedReader(new FileReader("/tmp/" + current_user + "/splits/S" + split_number + ".txt"));

        while ((line = br.readLine()) != null) {

            for (String word : line.split(" ")) {

                writer.append(word + " 1\n");

            }
        }
        br.close();
        writer.close();

    }

    public static void map_shuffle(String split_number, String current_user) throws IOException, InterruptedException {
        //first improvement of the project mainline
        //do map and shuffle in the same phase in order to reduce IO on disk

        String line;
        //create directory where the files will be put
        functions.check_or_create_dir("/tmp/" + current_user + "/shuffles_received/", null);
        BufferedReader br = new BufferedReader(new FileReader("/tmp/" + current_user + "/splits/S" + split_number + ".txt"));
        //get the machine on the cluster list
        HashMap<Integer, String> my_cluster = get_machines(current_user);
        ExecutorService executorService = Executors.newCachedThreadPool();
        Set<String> hashset = new HashSet<>();
        int nb_machines = my_cluster.size();
        functions.check_or_create_dir("/tmp/" + current_user + "/shuffles/", null);

        //create a HashMap associating a shuffle number to a bufferedwritter
        //avoid opening and closing bufferwritter between each word
        //very efficient way to do
        HashMap<Integer, BufferedWriter> my_map = new HashMap<Integer, BufferedWriter>();
        for (int iter = 0; iter < nb_machines; iter++) {
            my_map.put(iter, new BufferedWriter(new FileWriter("/tmp/" + current_user + "/shuffles/" + iter + "-" + split_number + ".txt", true)));
        }
        //get each word and put them in the good file
        while ((line = br.readLine()) != null) {

            for (String word : line.split(" ")) {

                int receviver = Math.abs(word.hashCode() % nb_machines);
                my_map.get(receviver).append(word + " \n");
                hashset.add(String.valueOf(receviver));

            }
        }
        //close all writers
        for (int iter = 0; iter < nb_machines; iter++) {
            my_map.get(iter).close();
        }
        //then send file to the receiver machine using multiprocessing
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        for (String hash : hashset) {
            callables.add(new deploy_file_callable(my_cluster.get(Integer.valueOf(hash)), "/tmp/" + current_user + "/shuffles/" + hash + "-" + split_number + ".txt", "/tmp/" + current_user + "/shuffles_received/", split_number));
        }
        @SuppressWarnings("unused")
        List<Future<String>> futures = executorService.invokeAll(callables);
        executorService.shutdown();
        br.close();
    }


    public static void shuffle(String split_number, String current_user) throws IOException, InterruptedException {
        //classic shuffle phase from the project

        String line;
        HashMap<Integer, String> my_cluster = get_machines(current_user);
        ExecutorService executorService = Executors.newCachedThreadPool();
        Set<String> hashset = new HashSet<>();
        int nb_machines = my_cluster.size();
        functions.check_or_create_dir("/tmp/" + current_user + "/shuffles/", null);
        BufferedReader br = new BufferedReader(new FileReader("/tmp/" + current_user + "/maps/UM" + split_number + ".txt"));
        //create a HashMap associating a shuffle number to a bufferedwritter
        //avoid opening and closing bufferwritter between each word
        //very efficient way to do
        HashMap<Integer, BufferedWriter> my_map = new HashMap<Integer, BufferedWriter>();
        for (int iter = 0; iter < nb_machines; iter++) {
            my_map.put(iter, new BufferedWriter(new FileWriter("/tmp/" + current_user + "/shuffles/" + iter + "-" + split_number + ".txt", true)));
        }
        //get each word and put them in the good file
        while ((line = br.readLine()) != null) {

            String word = line.split(" ")[0];
            int receviver = Math.abs(word.hashCode() % nb_machines);
            my_map.get(receviver).append(word + " \n");
            hashset.add(String.valueOf(receviver));
        }
        //close all writers
        for (int iter = 0; iter < nb_machines; iter++) {
            my_map.get(iter).close();
        }
        //then send file to the receiver machine using multiprocessing
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        for (String hash : hashset) {
            callables.add(new deploy_file_callable(my_cluster.get(Integer.valueOf(hash)), "/tmp/" + current_user + "/shuffles/" + hash + "-" + split_number + ".txt", "/tmp/" + current_user + "/shuffles_received/", split_number));
        }
        @SuppressWarnings("unused")
        List<Future<String>> futures = executorService.invokeAll(callables);
        br.close();
        executorService.shutdown();
    }

    public static void map_reduce_shuffle(String split_number, String current_user) throws IOException, InterruptedException, ExecutionException {
        //second improvement in the project mainline
        //in order to reduce the time taken by the scp transfer between machine apply a reduce before puting in shuffle
        //shuffles then became much much smaller
        functions.check_or_create_dir("/tmp/" + current_user + "/shuffles_received/", null);
        functions.check_or_create_dir("/tmp/" + current_user + "/shuffles/", null);
        ExecutorService executorService = Executors.newCachedThreadPool();
        //get a a dict of word and occurency using the classic system function with multiprocessing
        HashMap<String, Integer> my_dict = classic_system_for_comparison.counter_multiprocessing("/tmp/" + current_user + "/splits/S" + split_number + ".txt", 12);
        HashMap<Integer, String> my_cluster = get_machines(current_user);
        //then do exactly as the shuffle phase do but using the value inside the dict instead of the one in the file
        int nb_machines = my_cluster.size();
        HashMap<Integer, BufferedWriter> my_map = new HashMap<Integer, BufferedWriter>();
        Set<String> hashset = new HashSet<>();
        for (int iter = 0; iter < nb_machines; iter++) {
            my_map.put(iter, new BufferedWriter(new FileWriter("/tmp/" + current_user + "/shuffles/" + iter + "-" + split_number + ".txt", true)));
        }
        for (String word : my_dict.keySet()) {

            int receviver = Math.abs(word.hashCode() % nb_machines);
            my_map.get(receviver).append(word + " " + my_dict.get(word) + "\n");
            hashset.add(String.valueOf(receviver));
        }
        for (int iter = 0; iter < nb_machines; iter++) {
            my_map.get(iter).close();
        }
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        for (String hash : hashset) {

            callables.add(new deploy_file_callable(my_cluster.get(Integer.valueOf(hash)), "/tmp/" + current_user + "/shuffles/" + hash + "-" + split_number + ".txt", "/tmp/" + current_user + "/shuffles_received/", split_number));
        }
        @SuppressWarnings("unused")
        List<Future<String>> futures = executorService.invokeAll(callables);
        executorService.shutdown();
    }

    public static void reduce_for_map_reduce_shuffle(String split_number, String Master_node, String current_user) throws IOException, InterruptedException {
        //reduce phase modify to apply on the shuffles getted after a map_reduce_shuffle phase
        //get the list of all the received shuffle and then merged them in a new dict
        String[] commande = {"ls", "/tmp/" + current_user + "/shuffles_received/"};
        ExecutorService executorService = Executors.newCachedThreadPool();
        ProcessBuilder pb = new ProcessBuilder(commande);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        p.waitFor();
        BufferedReader br = new BufferedReader(new InputStreamReader((p.getInputStream())));
        String line;
        HashMap<String, Integer> reduce_result = new HashMap<String, Integer>();
        String word = null;
        String file;
        String occurence;
        while ((file = br.readLine()) != null) {
            BufferedReader br2 = new BufferedReader(new FileReader("/tmp/" + current_user + "/shuffles_received/" + file));
            while ((line = br2.readLine()) != null) {
                word = line.split(" ")[0];
                occurence = line.split(" ")[1];
                reduce_result.put(word, reduce_result.getOrDefault(word, 0) + Integer.valueOf(occurence));
            }
            br2.close();
        }
        functions.check_or_create_dir("/tmp/" + current_user + "/reduce/", null);
        BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/" + current_user + "/reduce/R" + split_number + ".txt", true));
        //copy the result on a file and send it back to the master node
        for (String mot : reduce_result.keySet()) {
            writer.append(mot + " " + reduce_result.get(mot) + "\n");
        }
        writer.close();
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        callables.add(new deploy_file_callable(Master_node, "/tmp/" + current_user + "/reduce/R" + split_number + ".txt", "/tmp/" + current_user + "/Reduce_received/", split_number));
        @SuppressWarnings("unused")
        List<Future<String>> futures = executorService.invokeAll(callables);
        p.destroy();
        executorService.shutdown();


    }


    public static void reduce(String split_number, String Master_node, String current_user) throws IOException, InterruptedException {
        //classic reduce function from the projet
        //get the list of all the shuffles received and then create a dict and count occurency of word
        //then create a result file and send it to the master node
        String[] commande = {"ls", "/tmp/" + current_user + "/shuffles_received/"};
        ExecutorService executorService = Executors.newCachedThreadPool();
        ProcessBuilder pb = new ProcessBuilder(commande);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        p.waitFor();
        BufferedReader br = new BufferedReader(new InputStreamReader((p.getInputStream())));
        String line;
        HashMap<String, Integer> reduce_result = new HashMap<String, Integer>();
        String word = null;
        String file;
        while ((file = br.readLine()) != null) {
            BufferedReader br2 = new BufferedReader(new FileReader("/tmp/" + current_user + "/shuffles_received/" + file));
            while ((line = br2.readLine()) != null) {
                word = line;
                reduce_result.put(word, reduce_result.getOrDefault(word, 0) + 1);
            }
            br2.close();
        }
        functions.check_or_create_dir("/tmp/" + current_user + "/reduce/", null);
        BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/" + current_user + "/reduce/R" + split_number + ".txt", true));
        for (String mot : reduce_result.keySet()) {
            writer.append(mot + " " + reduce_result.get(mot) + "\n");
        }
        writer.close();
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        callables.add(new deploy_file_callable(Master_node, "/tmp/" + current_user + "/reduce/R" + split_number + ".txt", "/tmp/" + current_user + "/Reduce_received/", split_number));
        @SuppressWarnings("unused")
        List<Future<String>> futures = executorService.invokeAll(callables);
        p.destroy();
        executorService.shutdown();
    }


    public static HashMap<Integer, String> get_machines(String current_user) throws NumberFormatException, IOException {
        //function that read the machines file received from the master node in order to get the cluster machine list
        String line;
        BufferedReader br = new BufferedReader(new FileReader("/tmp/" + current_user + "/machines.txt"));
        HashMap<Integer, String> my_cluster = new HashMap<Integer, String>();
        while ((line = br.readLine()) != null) {
            Integer split_number = Integer.valueOf(line.split(" ")[0]);
            String machine = line.split(" ")[1];
            my_cluster.put(split_number, machine);
        }
        br.close();
        return my_cluster;

    }

    public static void gunzip_split(String split_number, String current_user) throws IOException, InterruptedException {
        String[] cmd = {"gunzip", "/tmp/" + current_user + "/splits/S" + split_number + ".txt.gz"};
        ProcessBuilder pb = new ProcessBuilder(cmd);
        Process p = pb.start();
        p.waitFor();
        p.destroy();
    }
}
