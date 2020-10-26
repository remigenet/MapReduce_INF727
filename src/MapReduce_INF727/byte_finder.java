package MapReduce_INF727;

import static java.nio.file.StandardOpenOption.READ;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

public class byte_finder {
	
	public static ArrayList<Long> find_byte_split_position(String file, int nb_cut) throws IOException{
		// function used to find space in the text in order to don't cut word. the number of cut need to reduce the approximate
		//split size under the max integer value
		int pos;
		MappedByteBuffer bb;
		FileChannel in = FileChannel.open(Paths.get(file), READ) ;
		long split_size_approximate= in.size()/nb_cut;
		ArrayList<Long> split_positions= new ArrayList<>();
		long split_place=0;
		split_positions.add( split_place);
		
		for(int split=0; split<nb_cut-1; split++) {
			
			long estimated_split_position=split_place+split_size_approximate;
			
			bb = in.map(FileChannel.MapMode.READ_ONLY,estimated_split_position-50,100);
			pos=0;
			while(bb.get(pos++)!=' ');
			split_place=estimated_split_position-50+pos;
			split_positions.add( split_place);
			
		}
		split_positions.add(in.size());
		
		return split_positions;
		
	}
	
	public static HashMap<Integer,ArrayList<Long>> find_byte_split_position_for_multithread(String file, int nb_cut, int nb_thread) throws IOException{
		// function used to find space in the text in order to don't cut word. the number of cut need to reduce the approximate
		//split size under the max integer value
		//this function was created in order to cut process splits in smaller splits inside, in order to do multithreading
		//However as discuss in my report, the multithreading was not working fine because of thread safe trouble
		//but this function just work fine and can be use later
		int pos;
		MappedByteBuffer bb;
		FileChannel in = FileChannel.open(Paths.get(file), READ) ;
		long split_size_approximate=in.size()/nb_cut/nb_thread;
		
		long split_place=0;
		 HashMap<Integer,ArrayList<Long>>  split_positions_all_process= new HashMap<>();
		
		for(int split=0; split<nb_cut; split++) {
			ArrayList<Long> split_positions= new ArrayList<>();
			split_positions.add( split_place);
			for(int thread=0; thread<nb_thread; thread++) {

				long estimated_split_position=split_place+split_size_approximate;
				
				bb = in.map(FileChannel.MapMode.READ_ONLY,estimated_split_position-50,100);
				pos=0;
				while(bb.get(pos++)!=' ');
				split_place=estimated_split_position-50+pos;
				if(split==nb_cut-1 && thread==nb_thread-1) {
					split_positions.add( in.size());
				}
				else {
					split_positions.add( split_place);
				}
				
			
			}
			split_positions_all_process.put(split, split_positions);
		}
		
		
		return split_positions_all_process;
		
	}
}
