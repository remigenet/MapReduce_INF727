package MapReduce_INF727;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class splits_functions {

	public static void splits(String input_file, int nb_machines, String output_folder) throws FileNotFoundException, UnsupportedEncodingException {
		//first split function
		//not working on huge file because of strings limits
		//quite slow because to read and write all the file 
		int count=0;
		String text=functions.readFile_big_file(input_file);
		long nSentences = text.split("\n").length;
		double splitsize = (double)nSentences/(double)nb_machines;
		ArrayList<String> sentences= new ArrayList<>(Arrays.asList(text.split("\n")));
		PrintWriter writer=null;
		for (int row=0; row<nSentences; row++) {
			if ((double)row%splitsize<1) {
				if (count>0) {
					writer.close();
				}
				writer = new PrintWriter(output_folder+"S"+String.valueOf(count)+".txt", "UTF-8");
				count++;
			}
			writer.println(sentences.get(row));
						
	    		
		}
		writer.close();
	}
	
	
	public static void splits_for_large_file(String input_file, int nb_machines, String output_folder, boolean compression, String current_user) throws IOException, InterruptedException {
		//second version of the spliter function
		//use the linux split function to do the job
		//then rename the file getted to match S0, S1, ... names
		
		InputStream inputStream = new BufferedInputStream(new FileInputStream(new File(input_file)));
		File data =new File(input_file);
		ExecutorService executorService = Executors.newCachedThreadPool();
		long file_size=data.length();
		String size=String.valueOf(file_size/(nb_machines-1));
		inputStream.close();
        String[] split_cmd = {"split","-C",size,"-u",input_file,"/tmp/"+current_user+"/splits/S"};
    	ProcessBuilder pb_split = new ProcessBuilder(split_cmd);
    	pb_split.redirectError();
		Process p = pb_split.start();
		p.waitFor();
		p.destroy();
		String[] commande = { "ls", "/tmp/"+current_user+"/splits/" };
	    ProcessBuilder pb2 = new ProcessBuilder(commande);
        pb2.redirectErrorStream(true);
        Process p2 = pb2.start();
        
        BufferedReader br = new BufferedReader(new InputStreamReader((p2.getInputStream())));
        
        String file;
        int split_number=0;
        Set<Callable<String>> callables = new HashSet<Callable<String>>();

        while ((file = br.readLine()) != null) {
        	String[] rename_cmd = { "mv", "/tmp/"+current_user+"/splits/"+file,"/tmp/"+current_user+"/splits/S"+split_number+".txt" };
        	if(compression) {
        		callables.add(new gzip_callable("/tmp/"+current_user+"/splits/S"+split_number+".txt"));
        	}
    	    pb2 = new ProcessBuilder(rename_cmd);
            pb2.redirectErrorStream(true);
            p2 = pb2.start();
            p2.waitFor();
            split_number++;
        }
        p2.destroy();
        System.out.println("Start compressing");
        if(compression) {
        	@SuppressWarnings("unused")
			List<Future<String>> futures = executorService.invokeAll(callables);
        }		
	}
   
   public static void spliter_multiproc(String bigFile, int nb_split, String output_folder, boolean compression, String current_user) throws IOException, InterruptedException {
	   //multiprocessing spliting functions
	   //more efficient than Linux function on small file <5Go but equal on other file
	   //doesn't work on very small file because of the split byte finding function
	   
	   ExecutorService executorService = Executors.newCachedThreadPool();
	   File data=new File(bigFile);
       long end=data.length();
       data=null;
       long size= end/nb_split;
       if(size>2147483647-100000) {
       	nb_split=(int) (end/(2147483647-100000));
       }
	   
	   ArrayList<Long> all_split_position=byte_finder.find_byte_split_position(bigFile, nb_split);
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        System.out.println(end);
        System.out.println(all_split_position);
        int iter=0;
   
        for(int split=0; split<nb_split; split++){
 
    		callables.add(new callable_spliter(bigFile, output_folder+"S"+iter+".txt", all_split_position.get(split), all_split_position.get(split+1) ));
    		iter++;
        }
       
       @SuppressWarnings("unused")
       List<Future<String>> futures = executorService.invokeAll(callables);
       callables.clear();
       if(compression) {
    	   for(int split=0; split<nb_split; split++){
        	   callables.add(new gzip_callable("/tmp/"+current_user+"/splits/S"+split+".txt"));
           }
       }
       futures = executorService.invokeAll(callables);
       executorService.shutdown();

   }
	
}
