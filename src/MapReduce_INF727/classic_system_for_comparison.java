package MapReduce_INF727;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class  classic_system_for_comparison{
	//classic system of wordcount to compare with mapreduce system
	//contain two way of dealing with wordcount, a classic sequential one and a multiprocessed one
    public static void main(String[] args) {



        try {
            FileReader fr = new FileReader(args[0]);
            BufferedReader br = new BufferedReader(fr);
            if(args[1]=="classic") {
            		HashMap<String, Integer> my_dict = word_count_dict(br);
            		create_result_file(my_dict, "classic", args[3]);
            }
            else if (args[1]=="multi") {
            	HashMap<String, Integer> my_dict = counter_multiprocessing(args[0], Integer.valueOf(args[2]));
            	create_result_file(my_dict, "multi", args[3]);
            }
        } catch (Exception e) {
            e.printStackTrace() ;
        }
    }

    public static HashMap<String, Integer> word_count_dict(BufferedReader br) throws IOException{
    	//Classic sequential version of wordcount
    	//store word and occurence in a HashMap for quick access and use bufferedreader for quick read
        HashMap<String, Integer> my_dict= new HashMap<String, Integer>();
        String line;
        while ((line=br.readLine())!=null){
        	for(String word: line.split(" ")) {
        		//count occurency by adding one to the current occurency of the word
        		my_dict.put(word,my_dict.getOrDefault(word,0)+1);
        	}
        }
        //remove space and empty from the dict
        my_dict.remove(" ");
        my_dict.remove("");
        return my_dict;
    }
    
    
    
    public static HashMap<String, Integer> counter_multiprocessing(String File, int nb_split) throws IOException, InterruptedException, ExecutionException {
    	//Same function but using multiprocessing to increase performance
    	ExecutorService executorService = Executors.newCachedThreadPool();
    	//Get the file size to check if the size of a split doesn't go beyond the max MappedByteBuffer size
        File data=new File(File);
        long end=data.length();
        data=null;
        long size= end/nb_split;
        if(size>2147483647-100000) {
        	nb_split=(int) (end/(2147483647-100000));
        }
        //Find the split position where the is no word to don't cut them in two
        ArrayList<Long> split_position=byte_finder.find_byte_split_position(File, nb_split);
        //initialise callable list and fill them with process between splits
        Set<Callable<HashMap<String, Integer>>> callables = new HashSet<Callable<HashMap<String, Integer>>>();
        for(int split=0; split <nb_split; split++){
            
        	callables.add(new wordcount_multiprocessing_callable(File, split_position.get(split), split_position.get(split+1)));

        }
        //launch the callable process and then merge the different dict in the result one
        List<Future<HashMap<String, Integer>>> futures = executorService.invokeAll(callables);
        HashMap<String, Integer> my_dict =new HashMap<String, Integer>();
        for(Future<HashMap<String, Integer>> futur : futures) {	
        	HashMap<String, Integer> part_dict=futur.get();        	
        	for(String word : part_dict.keySet()) {        		
        		my_dict.put(word, my_dict.getOrDefault(word, 0)+part_dict.get(word));
        	}
        }
        executorService.shutdown();
        my_dict.remove(" ");
        my_dict.remove("");
        
        return my_dict;

    }
    
   
    
	public static void create_result_file(HashMap<String, Integer> my_result, String type, String current_user) throws IOException, InterruptedException {
		//function used to create a txt file with the HashMap created during the wordcount
		//used bufferedwriter for quick write
		File file= new File("/tmp/"+current_user+"_resultat/resultat_sequential_"+type+".txt");
		file.delete();
		
		BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/"+current_user+"_resultat/resultat_sequential_"+type+".txt", true));
        for(String mot: my_result.keySet()) {
        	writer.append(mot+" "+my_result.get(mot)+"\n");
        }
        writer.close();
	}

    }
