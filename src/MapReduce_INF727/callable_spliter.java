package MapReduce_INF727;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

public class callable_spliter implements Callable<String> {
	//Callable class used to copy part of a file into another
	//used here in splittin process 
	private String bigFile;
	private String splitFile;
	private long start;
	private long end;

	
    public callable_spliter(String m, String sp,long s, long e){
        this.bigFile=m;
        this.splitFile=sp;
        this.start=s;
        this.end=e;
    }
    
	@Override
    public String call() throws IOException  {
    	
    	WritableByteChannel out = FileChannel.open(Paths.get(splitFile), CREATE, APPEND);
    	
    	FileChannel in = FileChannel.open(Paths.get(bigFile), READ);
    	
    	MappedByteBuffer bb = in.map(FileChannel.MapMode.READ_ONLY,start,end-start);
    	//used MappedByte Buffer instead of FileChannel transferTo option because quicker
    	//in.transferTo(start, end-start, out);
    	out.write(bb);
    	
    	
    	in.close();
    	out.close();
    	return null;
	    	
	    }

}
