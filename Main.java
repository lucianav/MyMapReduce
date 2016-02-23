//Luciana Viziru - 332CA

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Collections;


public class Main {

	public static void main(String[] args) throws NumberFormatException, IOException {

		//run arguments are saved into variables	
		int nThreads = Integer.valueOf(args[0]);
		String inputFileName = args[1];
		String outputFileName = args[2];
		
		try {
			//input file is opened
			BufferedReader in = new BufferedReader(new InputStreamReader(
					new FileInputStream(new File(inputFileName)), "UTF8"));
			
			//read input data
			int size = Integer.valueOf(in.readLine());
			int numberOfFiles = Integer.valueOf(in.readLine());
			final LinkedList<String> files = new LinkedList<String>();
			
			for (int i = 0; i < numberOfFiles; i++) {
				files.add(in.readLine());
			}
			//close input file
			in.close();
			
			//workpool object is created
			WorkPool workpool = new WorkPool(nThreads);
			
			//for every file
			for (String fileName : files) {

				File f = new File(fileName);
				//read file size
				long fileSize = f.length();
				//the number of fragments in file
				int numberOfTasks = (int) (fileSize / size);
				//map tasks are created, one for each fragment
				for (int k = 0; k < numberOfTasks; k++) {
					workpool.putWork(new MapTask(fileName, k * size, size));
				}

				//if a smaller fragment for the end of file is needed
				if (fileSize % size != 0) {
					//it is created
					workpool.putWork(new MapTask(fileName, numberOfTasks * size,
									 (int) (fileSize - numberOfTasks * size)));
				}
			}
			
			//all worker threads are created
			Worker[] workers = new Worker[nThreads];
			for (int i = 0; i < nThreads; i++) {
				workers[i] = new Worker(workpool);
			}
			
			//workers are started
			for (Worker w : workers) {
				w.start();
			}
			
			//while the reduce stage is not complete
			//sleep for the main thread
			while (!workpool.reduceReady) {
				Thread.currentThread().sleep(100);
			}
			
			//now the reduce stage is finished, get results 
			LinkedList<ReduceResult> results = workpool.getReduceResults();
			
			//results are sorted by rank and by order in input file
			Collections.sort(results, new Comparator<ReduceResult>() {

				@Override
				public int compare(ReduceResult o1, ReduceResult o2) {
					if (o1.getRang() < o2.getRang())
						return 1;
					if (o1.getRang() > o2.getRang())
						return -1;
					return Integer.valueOf(files.indexOf(
								o1.getFileName())).compareTo(Integer.valueOf(
										files.indexOf(o2.getFileName())));
				}
			});
			
			//open output file
			PrintWriter out  = new PrintWriter(new File(outputFileName));
			
			//for every reduce stage result, write to file
			for (ReduceResult r : results) {
				out.println(r.getFileName() + ";" + 
						String.format("%.2f", r.getRang()) + ";" + "["
								+ r.getMaxLength() + ","
								+ r.getNumberOfMaxWords() + "]");
			}
			//close output
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
