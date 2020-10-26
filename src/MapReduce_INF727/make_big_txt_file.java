package MapReduce_INF727;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class make_big_txt_file {
	//class used to multiply a text file into a bigger one
	//choose line randomly 
	//just change the multiplyer and the txt file used as u wanted in the code bellow
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		long multiplier;
		functions.check_or_create_dir("/tmp/data_file/", null);
		FileReader fr = new FileReader("bible.txt");
        BufferedReader br = new BufferedReader(fr);
        HashMap<Integer,String> my_dict= new HashMap<>();
        Integer count=0;
        String line;
        while ((line=br.readLine())!=null){


            my_dict.put(count,line);
            count++;
        }
		
        long init_size=my_dict.size();
        if(args.length>0) {
        	multiplier=Long.parseLong(args[0]);
        }
        else {
        	multiplier=50;
        }
        long nb_line_objectif=init_size*multiplier;
        BufferedWriter writer = new BufferedWriter( new PrintWriter("/tmp/data_file/bible_time"+ multiplier +".txt", StandardCharsets.UTF_8));
        for(long iter=0; iter<nb_line_objectif;iter++) {
        	int nombreAleatoire =  (int)(Math.random() * ((init_size) + 1));
        	writer.append(my_dict.get(nombreAleatoire)).append("\n");
        }
		writer.close();
		br.close();
		System.out.println("done");
	}
	
	

	
}
