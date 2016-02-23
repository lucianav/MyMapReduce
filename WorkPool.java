//Luciana Viziru - 332CA

import java.util.Collections;
import java.util.LinkedList;

public class WorkPool {

	int nThreads; //number of threads in workpool
	int nWaiting = 0; //number of threads waiting for task
	public boolean mapReady = false; //flag for complete map
	public boolean reduceReady = false; //flag for complete reduce
	
	//containers for tasks and results
	LinkedList<MapTask> mapTasks = new LinkedList<MapTask>();
	LinkedList<MapResult> mapResults = new LinkedList<MapResult>();
	
	LinkedList<ReduceTask> reduceTasks = new LinkedList<ReduceTask>();
	LinkedList<ReduceResult> reduceResults = new LinkedList<ReduceResult>();
	
	//getter for reduce stage results
	public LinkedList<ReduceResult> getReduceResults() {
		return reduceResults;
	}

	//constructor for workpool
	public WorkPool(int nThreads) {
		this.nThreads = nThreads;
	}

	//method for map task getting
	public synchronized MapTask getMapWork() {

		if (mapTasks.size() == 0) {	//workpool is empty
			nWaiting++;		//worker goes into waiting

			//if all workers are waiting, stage is complete
			if (nWaiting == nThreads) {
				mapReady = true;	//set flag
				nWaiting = 0;		//reset workpool
				startReduce();		//get ready for reduce stage

				notifyAll(); 	//notify all workers stage is complete
				return null;	//return null task
			} else {
				//no more tasks in pool, some may be in execution
				while (!mapReady && mapTasks.size() == 0) {
					try {
						this.wait();		//worker will wait until notify
					} catch(Exception e) {e.printStackTrace();}
				}
				
				//map stage is complete, return null task
				if (mapReady)
				    return null;

				//a task can be distributed, one more executing thread
				nWaiting--;
			}
		}
		return mapTasks.remove();	//return task for thread
	}

	private void startReduce() {

		//sortare dupa numele fisierului 
		Collections.sort(mapResults);
		//primul fisier din lista
		String fileName = mapResults.getFirst().getFileName();
		//rezultatele acestui fisier
		LinkedList<MapResult> resultsForFile = new LinkedList<MapResult>();
				
		for (MapResult r : mapResults) {
			//adaugarea in lista rezultate
			if (fileName == r.getFileName()) {
				resultsForFile.add(r);
			}
			else {
				//s-a trecut la un nou fisier, se creeaza ReduceTask  
				ReduceTask t = new ReduceTask(fileName, resultsForFile);
				this.putWork(t);
				fileName = r.getFileName();
				resultsForFile = new LinkedList<MapResult>();
				resultsForFile.add(r);
			}
		}
		//se creaza si reduce task pentru ultimul fisier
		ReduceTask t = new ReduceTask(fileName, resultsForFile);
		this.putWork(t);
	}
	
	//method for reduce task getting
	public synchronized ReduceTask getReduceWork() {

		if (reduceTasks.size() == 0) {		//workpool is empty
			nWaiting++;						//worker goes into waiting
			
			//if all workers are waiting, stage is complete
			if (nWaiting == nThreads) {
				reduceReady = true;		//set flag
				notifyAll();			//notify all workers
				return null;			//return null task
			} else {

				//no more tasks in pool, some may be in execution
				while (!reduceReady && reduceTasks.size() == 0) {
					try {
						this.wait();		//ask worker to wait
					} catch(Exception e) {e.printStackTrace();}
				}
				
				//reduce stage is complete, return null task
				if (reduceReady)
				    return null;

				 //a task can be distributed, one more executing thread
				nWaiting--;
			}
		}
		return reduceTasks.remove();			//return task for thread
	}

	//method for map result adding to workpool
	public synchronized void addResult(MapResult result) {
		mapResults.add(result);
	}
	
	//method for reduce result adding to workpool
	public synchronized void addResult(ReduceResult result) {
		reduceResults.add(result);
	}
	
	//method for adding map type task to pool
	synchronized void putWork(MapTask task) {

		mapTasks.add(task);		//add task to pool
		this.notify();			//notify waiting threads
	}
	
	//method for adding reduce type task to pool
	synchronized void putWork(ReduceTask task) {

		reduceTasks.add(task);	//add task to pool
		this.notify();			//notify  waiting workers
	}
}
