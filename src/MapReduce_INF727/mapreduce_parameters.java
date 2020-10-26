package MapReduce_INF727;

public class mapreduce_parameters {
	//split function to use, linux or multiproc
	public String split_function;
	//want compression of file after split before deployement
	public boolean compression;
	//map, map_shuffle or map_reduce_shuffle
	public String mode;
	
	
	public mapreduce_parameters(String s, boolean c, String m) {
		this.split_function=s;
		this.compression=c;
		this.mode=m;
	}
	
}
