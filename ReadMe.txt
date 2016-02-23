Luciana Viziru

MapReduce

My implementation uses 5 classes: Main, Workpool, Worker, MapTask,
MapResult, ReduceTask and ReduceResult. The classes used for the result of
Map an Reduce are in the MapTask and ReduceTask files. The methods for adding
and distributing tasks are synchronized in order to avoid concurrent access
to the task and result containers. The Workpool holds the containers for both
the tasks and their results. After the Map stage, the number of waiting threads
in the workpool is reset and a Reduce task is created for all the MapResults
of one file, in the startReduce() method. The Map tasks are created in the
Main class, at the beginning. After the Map stage starts, the main thread will
sleep until the file processing is finished. When the Reduce stage is complete,
it will print the information in MapResult. Before the output, the results
are sorted using a custom comparator.