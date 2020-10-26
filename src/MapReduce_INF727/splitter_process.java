package MapReduce_INF727;



import java.io.IOException;

import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;


public class splitter_process extends Thread{
	//function used to split process in thread during the splitting process
	//all threads of a process shared same in and out
	//however not used anymore because not thread safe at all

	private long start;
	private long end;
	private WritableByteChannel out;
	private FileChannel in;
	
	public splitter_process(long s, long e, WritableByteChannel out,FileChannel in){
        this.start=s;
        this.end=e;
        this.out=out;
        this.in=in;
    }
	
	public void run() {
		try {
		   in.transferTo(start, end-start, out);
	       
		} catch (IOException e) {
			System.out.println("error");
			System.out.println(e);
 	}
		
	}
}



