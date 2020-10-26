package MapReduce_INF727;

import static java.nio.file.StandardOpenOption.READ;

import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.Callable;

public class wordcount_multiprocessing_callable implements Callable<HashMap<String, Integer>>{
	//callable class for the multiprocessed wordcount
	private String bigFile;
	private long start;
	private long end;
	
    public wordcount_multiprocessing_callable(String m, long s, long e){
        this.bigFile=m;
        this.start=s;
        this.end=e;
    }
	
	@Override
	public HashMap<String, Integer> call() throws Exception {
		
		//Open channel to read the file
		FileChannel in = FileChannel.open(Paths.get(bigFile), READ) ;
		//open a MappedByteBuffer to read it
	    MappedByteBuffer bb=in.map(FileChannel.MapMode.READ_ONLY,start,end-start);
	    //create dict to store words and occurence
	    HashMap<String, Integer> my_dict= new HashMap<String, Integer>();
		  
		   
	    String line="";
	    int count=0;
        int pos=0;
        int start=0;
        int end=bb.remaining();
        CharBuffer charBuffer;
        //here read and count 1000 row by 1000 row to not depass Charbuffer capacity
        while(bb.hasRemaining() && pos<end){
            
            while(bb.get(pos++)!='\n' && pos<end);
            count++;
            if (count>1000 || pos>=end-1 ){
            	
	            bb.position(start).limit(pos);
	            charBuffer = Charset.forName("UTF-8").decode(bb);
	     
	            line=charBuffer.toString();
	            charBuffer=null;
	            bb.clear();
	            for (String sentence : line.split("\n")){
	            	for(String word: sentence.split(" ")) {
	            		
		            		my_dict.put(word, my_dict.getOrDefault(word, 0)+1);
		            	
	            	}
	            	
	            }
	       
	            start=pos;
	            line = "";
	            count=0;
	            
            }
            
        }
	
	return my_dict;
    	
    }
		
		
	

}
