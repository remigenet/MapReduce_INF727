package MapReduce_INF727;

import static java.nio.file.StandardOpenOption.READ;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.HashMap;

public class make_big_txt_file {
	//class used to multiply a text file into a bigger one
	//choose line randomly 
	//just change the multiplyer and the txt file used as u wanted in the code bellow
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		long lStartTime = System.currentTimeMillis();
		long multiplier;
		String input_file;
		String name;
		
        if(args.length>2) {
        	multiplier=Long.parseLong(args[0]);
        	input_file=args[1];
        	name=args[2];
        }else if(args.length>0) {
        	multiplier=Long.parseLong(args[0]);
        	input_file="bible.txt";
        	name="bible";
        }
        else {
        	multiplier=50;
        	input_file="bible.txt";
        	name="bible";
        }
        FileChannel in = FileChannel.open(Paths.get(input_file), READ) ;
        long file_size=in.size();
        in.close();
        float estimated_output_size=(float)(file_size*multiplier/ (Math.pow(1024, 3)));
        DecimalFormat df=new DecimalFormat("0.00");
        System.out.println("creating a input file from "+input_file+" with a multiplier of "+multiplier+" estimated output size: "+df.format(estimated_output_size)+"Go");
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
        long nb_line_objectif=init_size*multiplier;
        BufferedWriter writer = new BufferedWriter( new PrintWriter("/tmp/data_file/"+name+"_time"+ multiplier +".txt", StandardCharsets.UTF_8));
        for(long iter=0; iter<nb_line_objectif;iter++) {
        	int nombreAleatoire =  (int)(Math.random() * ((init_size) + 1));
        	writer.append(my_dict.get(nombreAleatoire)).append("\n");
        }
		writer.close();
		br.close();
		
		long lEndTime = System.currentTimeMillis();
		System.out.println("file created in "+((lEndTime-lStartTime)/1000)+" seconds");
	}
	
	

	
}
