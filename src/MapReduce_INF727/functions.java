package MapReduce_INF727;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class functions {
	//This class store all usefull functions that are called in the projet and that don't belong to a specific part
	public static void main(String[] args) {

	}
	
	
	public static String get_user() throws IOException {
		
		String[] commande = { "whoami" };
	    ProcessBuilder pb = new ProcessBuilder(commande);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        BufferedReader br = new BufferedReader(new InputStreamReader((p.getInputStream())));
        String name=br.readLine();
        p.destroy();
		
		return name;
		
	}
	
	public static void check_or_create_dir(String dir_path, String machine) throws IOException, InterruptedException {
		
		Process p=null;
			if (machine==null) {
				String[] create_dir_cmd= {"mkdir", "-p", dir_path};
				ProcessBuilder pb_mkdir = new ProcessBuilder(create_dir_cmd);
				pb_mkdir.redirectErrorStream();				
		    	p = pb_mkdir.start();
		        p.waitFor();		        
			}
			else {
				String[] create_dir_cmd= {"ssh",machine,"mkdir", "-p", dir_path};
				ProcessBuilder pb_mkdir = new ProcessBuilder(create_dir_cmd);
				pb_mkdir.redirectErrorStream();
		    	p = pb_mkdir.start();
		        p.waitFor();
			}
		p.destroy();
	}
	
	public static boolean ssh_working(String machine, String current_user) throws IOException, InterruptedException {
		//first version to test if a machine answer to ssh command, only return a boolean
		String[] commande = { "ssh", "-o StrictHostKeyChecking=no", machine, "hostname" };
	    ProcessBuilder pb = new ProcessBuilder(commande);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        boolean notTimeout = p.waitFor(5, TimeUnit.SECONDS);
        if(notTimeout) {
        	BufferedReader br2 = new BufferedReader(new InputStreamReader((p.getInputStream())));
            String name=br2.readLine();
            p.destroy();
            if (machine.equals(current_user+"@"+name)) {
            	return true;
            }
            else {
            	return false;
            } 
        }
        else {
        	p.destroy();
        	System.out.println("timedout on "+machine);
        	return false;
        	
        }
	}
	
public static long ssh_working_speed_test(String machine, String current_user) throws IOException, InterruptedException {
		//same function as above but here return the time taken by the machine to answer
		//used to select the quickest machine for making the cluster
		String[] commande = { "ssh", "-o StrictHostKeyChecking=no", machine, "hostname" };
	    ProcessBuilder pb = new ProcessBuilder(commande);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        long lStartTime=System.currentTimeMillis();
        boolean notTimeout = p.waitFor(10, TimeUnit.SECONDS);
        long lEndTime=System.currentTimeMillis();
        if(notTimeout) {
        	BufferedReader br2 = new BufferedReader(new InputStreamReader((p.getInputStream())));
            String name=br2.readLine();
            p.destroy();
            if (machine.equals(current_user+"@"+name)) {
            	return lEndTime-lStartTime;
            }
            else {
            	return -1;
            }
            
        }
        else {
        	p.destroy();
        	return -1;
        	
        }
	}
	
	public static String readFile(String filename) {
		try {
			FileInputStream stream = new FileInputStream(new File(filename));
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0,
					fc.size());
			stream.close();
			return Charset.defaultCharset().decode(bb).toString();
		} catch (IOException e) {
			System.out.println("Erreur lors de l'accès au fichier " + filename);
			System.exit(1);
		}
		return null;
	}
	
	public static String readFile_big_file(String filename) throws FileNotFoundException {

		FileReader fr = new FileReader(filename);
        BufferedReader br = new BufferedReader(fr);
        Scanner sc = new Scanner(br);
        String text="";
        
        while (sc.hasNextLine()) {
        	text+=sc.nextLine()+"\n";
        	
        }
        sc.close();
        return text;
	}
	
	public static ArrayList<String> get_machine(String machines_list_file) throws IOException{
		//function used to store the machine list in a given file in a ArrayList object
		BufferedReader br = new BufferedReader(new FileReader(machines_list_file));
		ArrayList<String> machines= new ArrayList<String>();
		String machine;
		while ((machine = br.readLine()) != null) {
			machines.add(machine);
		}
		br.close();
		return machines;
	}
	
	public static void move_element(String file, String destination) throws IOException, InterruptedException {
		//Function used in order to move element from a place to another 
		String[] copy_cmd = {"scp", file,destination};
    	ProcessBuilder pb_copy = new ProcessBuilder(copy_cmd);
        Process p = pb_copy.start();
        p.waitFor();
        p.destroy();
		
	}
	public static ArrayList<Integer> delete_element(Integer element, ArrayList<Integer> my_list){
		//function used to delete an element based on it's value on an arraylist
		//used on the first deployment method but not anymore
		//the function still can be very usefull
		for(int iter=0; iter<my_list.size();iter++) {
			if(my_list.get(iter).equals(element)) {
				my_list.remove(iter);
			}
		}
		return my_list;
	}
	
	public static ArrayList<String> delete_element(String element, ArrayList<String> my_list){
		//same function as above but for different type of arraylist
		for(int iter=0; iter<my_list.size();iter++) {
			if(my_list.get(iter).equals(element)) {
				my_list.remove(iter);
			}
		}
		return my_list;
	}

	public static void create_result_file(HashMap<String, Integer> my_result, int nb_machines, String current_user) throws IOException, InterruptedException {
		//functions to create the result file after the mapreduce process
		functions.check_or_create_dir("/tmp/"+current_user+"_resultat/", null);
		BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/"+current_user+"_resultat/resultat"+Integer.valueOf(nb_machines)+".txt", true));
        for(String mot: my_result.keySet()) {
        	writer.append(mot+" "+my_result.get(mot)+"\n");
        }
        writer.close();
	}
}

