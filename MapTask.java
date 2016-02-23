//Luciana Viziru - 332CA

import java.util.HashMap;
import java.util.LinkedList;

public class MapTask {
	
	private String fileName;	//name of file to process
	private long offset;		//offset of file fragment
	private int size;			//size of fragment
	
	public MapTask() {}			//default constructor
	
	//constructor with arguments
	public MapTask(String fileName, long offset, int size) {

		this.fileName = fileName;
		this.offset = offset;
		this.size = size;
	}
	
	//getters for class members
	public String getFileName() {
		return fileName;
	}

	public long getOffset() {
		return offset;
	}

	public int getSize() {
		return size;
	}
}

//class for the result of a Map Task used in Reduce Task
class MapResult implements Comparable<MapResult>{

	private HashMap<Integer, Integer> fragmentHash;	//hash for file fragment
	private LinkedList<String> maximumLengthWords;	//list of maximum words
	private String fileName;						//name of file
	
	//constructor with fields
	public MapResult(HashMap<Integer, Integer> fragmentHash,
			LinkedList<String> maximumLengthWords, String fileName) {
		super();
		this.fragmentHash = fragmentHash;
		this.maximumLengthWords = maximumLengthWords;
		this.fileName = fileName;
	}

	//getters for class members
	public HashMap<Integer, Integer> getFragmentHash() {
		return fragmentHash;
	}

	public LinkedList<String> getMaximumLengthWords() {
		return maximumLengthWords;
	}

	public String getFileName() {
		return fileName;
	}

	//method used for task results sorting
	@Override
	public int compareTo(MapResult o) {
		return this.fileName.compareTo(o.fileName);
	}	
}