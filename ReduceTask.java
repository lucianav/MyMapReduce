//Luciana Viziru - 332CA

import java.util.LinkedList;

public class ReduceTask {
	
	private String fileName;	//name of file
	//results of map stage for file fragments
	private LinkedList<MapResult> resultsForFile;
	
	public ReduceTask() {}		//default constructor

	//constructor with parameters
	public ReduceTask(String fileName, LinkedList<MapResult> resultsForFile) {
		super();
		this.fileName = fileName;
		this.resultsForFile = resultsForFile;     }

	//getters for class members
	public String getFileName() {
		return fileName;
	}

	public LinkedList<MapResult> getResultsForFile() {
		return resultsForFile;
	}	
}

//class for Reduce Task result, used for output
class ReduceResult {
	
	private String fileName;	//name of file
	private double rang;		//rank of file
	private int maxLength;		//maximum lenght for words
	private int numberOfMaxWords;	//number of words with maxLength
	
	//constructor with parametrs
	public ReduceResult(String fileName, double rang,
						int maxLength, int numberOfMaxWords) {
		super();
		this.fileName = fileName;
		this.rang = rang;
		this.maxLength = maxLength;
		this.numberOfMaxWords = numberOfMaxWords;
	}

	//getters for class members
	public String getFileName() {
		return fileName;
	}

	public double getRang() {
		return rang;
	}

	public int getMaxLength() {
		return maxLength;
	}

	public int getNumberOfMaxWords() {
		return numberOfMaxWords;
	}
}
