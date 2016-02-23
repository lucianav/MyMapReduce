//Luciana Viziru - 332CA

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.StringTokenizer;

public class Worker extends Thread {

	private WorkPool workpool = null;			//workpool for thread
	private boolean mapIsFinished = false;		//flag for map phase end
	private boolean reduceIsFinished = false;	//flag for reduce phase end


	public Worker() {}		//default constructor

	//constructor with parameters
	public Worker(WorkPool workpool) {
		super();
		this.workpool = workpool;
	}

	//method for map processing
	private void processTask(MapTask task) {

		String[] words = readFragment(task);		//get words in fragment
		if (words != null) {
			
			//create result members
			HashMap<Integer,Integer> fragmentHash = new HashMap<Integer,Integer>();
			int maxLength = 0;
			LinkedList<String> maxLengthWords = new LinkedList<String>();
			
			//for every word
			for (String word : words) {
				//if length of word is already in hashmap
				if (fragmentHash.containsKey(word.length())) {
					//increment value
					int count = fragmentHash.get(word.length()) + 1;
					fragmentHash.put(word.length(), count);
				}
				else {
					//put value of 1 for length
					fragmentHash.put(word.length(), 1);
				}
				//if word has maxlength length, add it to list of max words
				if (word.length() == maxLength) {
					maxLengthWords.add(word);
				}
				//if a greater length is found, empty list and start a new one
				if (word.length() > maxLength) {
					maxLength = word.length();
					maxLengthWords.clear();
					maxLengthWords.add(word);
				}
			}
			//result for map task is created
			 MapResult result = new MapResult(fragmentHash, maxLengthWords,
			 												task.getFileName());
			 workpool.addResult(result);		//result is added to workpool
		}
	}

	//method for reading fragment in map task
	private String[] readFragment(MapTask task) {
		//string of word delimiters
		String delimiters = ";:/?~\\.,><~`[]{}()!@#$%^&-_+'=*|\" \t\n \r\n";

		try {
			//open file for reading
			RandomAccessFile read = new RandomAccessFile(task.getFileName(), "r");

			long offset = task.getOffset();	//set offset
			byte[] b = new byte[1];
			long begin = offset;
			//if fragment is not the beginning of file
			if (begin > 0) {
				//read previos character
				read.seek(begin - 1);
				b[0] = read.readByte();
				//while it is not a delimiter
				while (!delimiters.contains(new String(b, "UTF8"))) {
					//move forward
					begin ++;
					read.seek(begin - 1);
					b[0] = read.readByte();
				}
			}

			//initial index of fragment end
			long end = offset + task.getSize();

			//read last byte/character
			read.seek(end - 1);
			b[0] = read.readByte();
			//while it is not a delimiter
			while (!delimiters.contains(new String(b, "UTF8"))) {
				//move forward
				end ++;
				read.seek(end - 1);
				//if eof is reached, stop and exit loop
				if (read.getFilePointer() >= read.length()) {
					end --;
					break;
				}
				b[0] = read.readByte();
			}				

			//allocate buffer for whole fragment
			byte[] content = new byte[(int) (end - begin)];
			read.seek(begin);			//set file pointer
			read.read(content);			//read form file to buffer
			
			read.close();				//close file

			//content from byte array to string with utf8 encoding
			String contents = new String(content, "UTF8");

			//divide fragment into words with given delimiters
			StringTokenizer tokenizer = new StringTokenizer(contents, delimiters);
			//allocate word array
			String[] words = new String[tokenizer.countTokens()];
			//fill array with words
			for (int i = 0; i < words.length; i++) {
				words[i] = tokenizer.nextToken();
			}
			return words;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	//method for reduce processing
	private void processTask(ReduceTask task) {

		//hash and words of maximum length for file
		HashMap<Integer, Integer> fileHash = new HashMap<Integer, Integer>();
		LinkedList<String> maxLengthWords = new LinkedList<String>();
		
		int maxLength = 0;
		//for every map result, find global maxLength of words
		for (MapResult r : task.getResultsForFile()) {
			if (r.getMaximumLengthWords().size() > 0 && 
				r.getMaximumLengthWords().getFirst().length() > maxLength) {
				maxLength =  r.getMaximumLengthWords().getFirst().length();
			}
		}

		///for every possible word length
		for (int l = 1; l <= maxLength; l++) {
			int count = 0;		//reset counter
			//for every map result
			for (MapResult r : task.getResultsForFile()) {
				//if there are words of that length
				if (r.getFragmentHash().containsKey(l)) {
					count += r.getFragmentHash().get(l);//add number to count
				}
				//if there are words of maxLength, add them to global list
				if (r.getMaximumLengthWords().size() > 0 && 
					r.getMaximumLengthWords().getFirst().length() == maxLength) {
					maxLengthWords.addAll(r.getMaximumLengthWords());
				}
			}
			//if l lenght words were found, add them to file hash
			if (count > 0) {
				fileHash.put(l, count);
			}
		}
		
		double rank = 0;			//initialize rank
		long wordCount = 0;
		//for every possible word length
		for (int l = 1; l <= maxLength; l++) {
			//if there are words of l length
			if (fileHash.containsKey(l)) {
				//add to rank sum
				rank += myFibonacci(l + 1) * fileHash.get(l);
				wordCount += fileHash.get(l);
			}
		}
		//divide rank sum by total number of words in file
		rank /= wordCount;

		//number of maxLength words
		int numberOfMaxWords = new HashSet<String>(maxLengthWords).size();
		
		//only keep first two decimals
		int aux = (int) (rank * 100);
		rank = (double )aux / 100.0;
		
		//create Reduce phase result
		ReduceResult result = new ReduceResult(task.getFileName(), rank,
												maxLength, numberOfMaxWords);
		workpool.addResult(result);		//add result to workpool
	}
	
	//method for fibonnaci sequence terms > 0
	private int myFibonacci(int n) {
		if (n == 1 || n == 2)
			return 1;
		
		return myFibonacci(n - 1) + myFibonacci(n - 2);
	}
	
	//run method for thread
	@Override
	public void run() {
		super.run();

		while(true) {
			//if map pahse is not over
			if (!mapIsFinished) {
				//try to get map task
				MapTask task = workpool.getMapWork();
				//task is null
				if (task == null) {
					//map phase is over, set flag and go to beginning of loop
					mapIsFinished = true;
					continue;
				}
				//a task was returned, process it
				processTask(task);
			}
			else {
				//map is over and reduce is running
				if (!reduceIsFinished) {
					//try to get reduce task
					ReduceTask task = workpool.getReduceWork();
					//task is null
					if (task == null) {
						//set flag for finished reduce and exit loop
						reduceIsFinished = true;
						break;
					}
					//a task was returned, process it
					processTask(task);
				}
			}
		}
	}
}
