package MapReduce_INF727;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class file_comparator {
	//class used to compare the result of different wordcount method
	
	public static void main(String[] args) {

	}
	
	public static boolean compare_file(String file_1, String file_2) throws NumberFormatException, IOException {
		//create two HashMap containing word and occurence in each file
		//then check if for each key in first hashmap the value associated to this key equal the one for the same key in second dict
		
		HashMap<String, Integer> dict_1=get_dict(file_1);
		HashMap<String, Integer> dict_2=get_dict(file_2);
		boolean same=true;
        for(String word: dict_1.keySet()) {
        	if(dict_1.get(word).equals(dict_2.get(word))==false) {
        		same=false;
        		System.out.println(word+" "+dict_1.get(word)+" "+dict_2.get(word));
        	}
        }
        
		return same;
		
	}
	
	public static HashMap<String, Integer> get_dict(String file) throws NumberFormatException, IOException{
		//function that create the HashMap of word and occurence with the file
		HashMap<String, Integer> reduce_result=new HashMap<String, Integer>();
		BufferedReader br = new BufferedReader(new FileReader(file));
    	int count=0;
    	String line;
    	String word;
    	while ((line = br.readLine()) != null) {
    		
    		ArrayList<String> result= new ArrayList<>(Arrays.asList(line.split(" ")));
 
        		word=result.get(0);
        		count=Integer.valueOf(result.get(result.size()-1));

        		reduce_result.put(word, count);
    		//}
    	}	
    	br.close();
    	return reduce_result;
	}
}
