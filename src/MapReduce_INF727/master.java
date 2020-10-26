package MapReduce_INF727;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class master {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        //main to launch for the project

        //find who is the user to adapt folder name during the whole project, only the Inputs for slave jar need to be modify
        String current_user = functions.get_user();

        //create a txt file, if already have one comment this part but change the Input path
        //use the file bible.txt that makes roughly 4Mo, a 500 multiplier in the create_file_cmd represent a 1.9Go output file
        String[] create_file_cmd = {"2000"};
        make_big_txt_file.main(create_file_cmd);

        //select your Inputs bellow
        ArrayList<String> machines = functions.get_machine("Machines_TP.csv");
        String slave_jar_path = "/cal/homes/rgenet/slave.jar";
        String Input = "/tmp/data_file/bible_time" + create_file_cmd[0] + ".txt";

        //select your parameters :
        //first : spliting function takes "multiproc" or "linux"
        //second: compression take true or false
        //third: mode takes "classic", "map_shuffle" or "map_reduce_shuffle"
        mapreduce_parameters my_parameters = new mapreduce_parameters("multiproc", false, "map_reduce_shuffle");

        System.out.println("Launching map reduce system");
        //launch map reduce
        int machines_getted = launch_map_reduce(machines, slave_jar_path, 11, Input, my_parameters, current_user);


        //launch classic system with sequential version
        System.out.println("Using wordcount sequential");
        launch_classic_system(Input, "classic", null, current_user);
        //sleep to let CPU cool
        Thread.sleep(20000);
        //launch classic system with multiprocessing version
        System.out.println("Using wordcount multiprocessed");
        launch_classic_system(Input, "multi", "12", current_user);

        //check if all files are equals
        System.out.println("Résultat test de similarité des resultat du systeme classique multithread et séquentiel: " + file_comparator.compare_file("/tmp/" + current_user + "_resultat/resultat_sequential_multi.txt", "/tmp/" + current_user + "_resultat/resultat_sequential_classic.txt"));
        System.out.println("Résultat test de similarité des resultat du systeme classique multithread et MapReduce: " + file_comparator.compare_file("/tmp/" + current_user + "_resultat/resultat_sequential_multi.txt", "/tmp/" + current_user + "_resultat/resultat" + machines_getted + ".txt"));
        System.out.println("Résultat test de similarité des resultat du systeme classique séquentiel et MapReduce: " + file_comparator.compare_file("/tmp/" + current_user + "_resultat/resultat" + machines_getted + ".txt", "/tmp/" + current_user + "_resultat/resultat_sequential_classic.txt"));

    }

    public static int launch_map_reduce(ArrayList<String> machines, String slave_jar_path, int nb_machines, String input_file, mapreduce_parameters parameters, String current_user) throws IOException, InterruptedException, ExecutionException {
        // main function calling all the different par of the mapreduce in function of parameters choose

        //Initializing variables for counting time of each phase
        long lStartTime;
        long AllStartTime = System.currentTimeMillis();
        long lEndTime;
        long time_elapsed;
        HashMap<String, Integer> my_result = null;


        //prepare the folders 
        lStartTime = System.currentTimeMillis();
        clean.clean_here("/tmp/" + current_user + "/");
        functions.check_or_create_dir("/tmp/" + current_user + "/Reduce_received/", null);
        functions.check_or_create_dir("/tmp/" + current_user + "/splits/", null);

        //send ssh connection to all the machines list give and return a machine_cluster object with the one responding the more quickly
        lStartTime = System.currentTimeMillis();
        System.out.println("Creating the cluster");
        machine_cluster my_cluster = master_cluster_and_deployement.find_cluster(machines, nb_machines, "/tmp/" + current_user + "/splits/", slave_jar_path, (float) 0.2, current_user);
        System.out.println("the cluster is:");
        System.out.println(my_cluster.machine_used);
        nb_machines = my_cluster.machine_used.size();
        lEndTime = System.currentTimeMillis();
        time_elapsed = lEndTime - lStartTime;
        System.out.println("Creating cluster elapsed time in s: " + time_elapsed / 1000);


        System.out.println("start splitting");
        lStartTime = System.currentTimeMillis();
        if (parameters.split_function == "multiproc") {
            splits_functions.spliter_multiproc(input_file, nb_machines, "/tmp/" + current_user + "/splits/", parameters.compression, current_user);
        } else if (parameters.split_function == "linux") {
            splits_functions.splits_for_large_file(input_file, nb_machines, "/tmp/" + current_user + "/splits/", parameters.compression, current_user);
        } else {
            System.out.println("Invalid split function, please select a valid parameter");
            System.exit(1);
        }

        lEndTime = System.currentTimeMillis();
        time_elapsed = lEndTime - lStartTime;
        System.out.println("Preparing folder and splits elapsed time in s: " + time_elapsed / 1000);

        //deploy the split on the cluster
        System.out.println("launch deployment");
        lStartTime = System.currentTimeMillis();
        my_cluster = master_cluster_and_deployement.deploy_split_on_cluster(my_cluster, "/tmp/" + current_user + "/splits/", slave_jar_path, parameters.compression, current_user);
        lEndTime = System.currentTimeMillis();
        time_elapsed = lEndTime - lStartTime;
        System.out.println("Deployment on cluster elapsed time in seconds: " + time_elapsed / 1000);

        //unzip file on machines if compressed
        if (parameters.compression) {
            my_cluster = master_map_and_shuffles_functions.gunzip_file(my_cluster, "/tmp/" + current_user + "/splits/", slave_jar_path, current_user);
        }
        //launch map phase, or map_shuffle, or map_reduce_shuffle depending on your choice
        if (parameters.mode == "classic") {

            //launch map phase
            System.out.println("launch map");
            lStartTime = System.currentTimeMillis();
            my_cluster = master_map_and_shuffles_functions.launch_map(my_cluster, "/tmp/" + current_user + "/splits/", slave_jar_path, parameters.compression, current_user);
            lEndTime = System.currentTimeMillis();
            time_elapsed = lEndTime - lStartTime;
            System.out.println("mapping on cluster elapsed time in seconds: " + time_elapsed / 1000);

            //launch shuffle phase
            System.out.println("launch shuffle");
            lStartTime = System.currentTimeMillis();
            my_cluster = master_map_and_shuffles_functions.launch_shuffle(my_cluster, "/tmp/" + current_user + "/splits/", slave_jar_path, parameters.compression, current_user);
            lEndTime = System.currentTimeMillis();
            time_elapsed = lEndTime - lStartTime;
            System.out.println("shuffling on cluster elapsed time in seconds: " + time_elapsed / 1000);

            //launch reduce
            System.out.println("launch reduce");
            lStartTime = System.currentTimeMillis();
            my_result = master_reduce.launch_reduce(my_cluster, "/tmp/" + current_user + "/splits/", slave_jar_path, true, parameters.compression, current_user);
            lEndTime = System.currentTimeMillis();
            time_elapsed = lEndTime - lStartTime;
            System.out.println("reducing elapsed time in seconds: " + time_elapsed / 1000);

        } else if (parameters.mode == "map_shuffle") {

            //launch map shuffle
            System.out.println("launch map shuffle");
            lStartTime = System.currentTimeMillis();
            my_cluster = master_map_and_shuffles_functions.launch_map_shuffle(my_cluster, "/tmp/" + current_user + "/splits/", slave_jar_path, parameters.compression, current_user);
            lEndTime = System.currentTimeMillis();
            time_elapsed = lEndTime - lStartTime;
            System.out.println("mapping_shuffling on cluster elapsed time in seconds: " + time_elapsed / 1000);

            //launch reduce
            System.out.println("launch reduce");
            lStartTime = System.currentTimeMillis();
            my_result = master_reduce.launch_reduce(my_cluster, "/tmp/" + current_user + "/splits/", slave_jar_path, true, parameters.compression, current_user);
            lEndTime = System.currentTimeMillis();
            time_elapsed = lEndTime - lStartTime;
            System.out.println("reducing elapsed time in seconds: " + time_elapsed / 1000);

        } else if (parameters.mode == "map_reduce_shuffle") {

            //launch map reduce shuffle
            System.out.println("launch map reduce shuffle");
            lStartTime = System.currentTimeMillis();
            my_cluster = master_map_and_shuffles_functions.launch_map_reduce_shuffle(my_cluster, "/tmp/" + current_user + "/splits/", slave_jar_path, parameters.compression, current_user);
            lEndTime = System.currentTimeMillis();
            time_elapsed = lEndTime - lStartTime;
            System.out.println("mapping reducing shuffling on cluster elapsed time in seconds: " + time_elapsed / 1000);

            //launch reduce
            System.out.println("launch reduce");
            lStartTime = System.currentTimeMillis();
            my_result = master_reduce.launch_reduce_for_map_reduce_shuflle(my_cluster, "/tmp/" + current_user + "/splits/", slave_jar_path, true, parameters.compression, current_user);
            lEndTime = System.currentTimeMillis();
            time_elapsed = lEndTime - lStartTime;
            System.out.println("reducing elapsed time in seconds: " + time_elapsed / 1000);

        } else {
            System.out.println("parameters mode unknown, select a valid parameter");
            System.exit(1);
        }

        //create the result file
        System.out.println("creating_result_file");
        lStartTime = System.currentTimeMillis();
        functions.create_result_file(my_result, nb_machines, current_user);
        lEndTime = System.currentTimeMillis();
        time_elapsed = lEndTime - lStartTime;
        System.out.println("creating result file elapsed time in seconds: " + time_elapsed / 1000);

        long AllEndTime = System.currentTimeMillis();
        time_elapsed = AllEndTime - AllStartTime;
        System.out.println("finish");
        System.out.println("All mapreduced process elapsed time in seconds: " + time_elapsed / 1000);
        System.out.println();
        System.out.println();
        //return the number of machine, needed to find the file in the file_comparison function
        return nb_machines;
    }

    public static void launch_classic_system(String Input, String method, String nb_thread, String current_user) {
        //launch the classic systeme in order
        long lStartTime;
        long lEndTime;
        long time_elapsed;
        lStartTime = System.currentTimeMillis();
        String[] path = {Input, method, nb_thread, current_user};
        classic_system_for_comparison.main(path);
        lEndTime = System.currentTimeMillis();
        time_elapsed = lEndTime - lStartTime;
        System.out.println("time elapsed (ms) : " + time_elapsed);
        //return time_elapsed;
    }

}
