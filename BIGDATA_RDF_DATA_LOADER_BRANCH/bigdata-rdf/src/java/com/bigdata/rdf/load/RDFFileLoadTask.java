package com.bigdata.rdf.load;

import java.io.File;
import java.io.FilenameFilter;
import java.io.Serializable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.load.RDFDataLoadMaster.JobState;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.rio.AsynchronousStatementBufferFactory;
import com.bigdata.rdf.rio.IAsynchronousWriteStatementBufferFactory;
import com.bigdata.rdf.rio.AsynchronousStatementBufferWithoutSids2.AsynchronousWriteBufferFactoryWithoutSids2;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.FederationCallable;
import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.jini.JiniFederation;

/**
 * Task reads files from the file system, loads them into an
 * {@link ITripleStore}, and then deletes the files if they have been loaded
 * successfully. This is a non-transactional bulk load using unisolated writes.
 * If the writes succeed then the client knows that the data are on stable
 * storage and may safely delete the source files. This task may be used in
 * conjunction with any process that writes files into a known directory on the
 * hosts of a cluster. The task will continue until canceled.
 * <p>
 * Note: Counters reporting the progress of this task will be attached to the
 * data service on which this task is executing.
 * <p>
 * Note: When loading files from a <em>local</em> file system, this task can
 * not handle the death of the service on which it is running.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <S>
 *            The generic for the {@link JobState}.
 * @param <V>
 *            The generic type of the client state (stored in zookeeper).
 */
public class RDFFileLoadTask<S extends JobState, V extends Serializable>
        extends FederationCallable<Void> implements Serializable {

    final protected transient static Logger log = Logger
            .getLogger(RDFFileLoadTask.class);

    /**
     * 
     */
    private static final long serialVersionUID = 6787939197771556658L;

    protected final S jobState;

    protected final int clientNum;

    public String toString() {

        return getClass().getName() + "{clientNum=" + clientNum + "}";

    }

    public RDFFileLoadTask(final S jobState, final int clientNum) {

        this.jobState = jobState;

        this.clientNum = clientNum;

    }

    /**
     * The federation object used by the {@link IRemoteExecutor} on which this
     * task is executing.
     */
    public JiniFederation getFederation() {

        return (JiniFederation) super.getFederation();

    }

    /**
     * {@link ConcurrentDataLoader}
     */
    public Void call() throws Exception {

        final JiniFederation fed = getFederation();

        final AbstractTripleStore tripleStore = (AbstractTripleStore) fed
                .getResourceLocator()
                .locate(jobState.namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

            throw new RuntimeException("KB not found: namespace="
                    + jobState.namespace);

        }

//        // optionally use asynchronous writes on the statement indices.
//        final IAsynchronousWriteStatementBufferFactory<BigdataStatement> statementBufferFactory = jobState.asynchronousWrites ? new AsynchronousWriteBufferFactoryWithoutSids2<BigdataStatement,File>(
//                (ScaleOutTripleStore) tripleStore,
//                jobState.asynchronousWritesProducerChunkSize,
//                jobState.valuesInitialCapacity,
//                jobState.bnodesInitialCapacity
////                jobState.syncRPCForTERM2ID
//                )
//                : null;
//
//      // Setup the task factory.
//      final RDFLoadTaskFactory taskFactory = new RDFLoadTaskFactory(
//              tripleStore, 
//              jobState.parserValidates, jobState.deleteAfter,
//              jobState.fallback, statementBufferfactory);
//
//      // Setup loader.
//      final ConcurrentDataLoader loader = new ConcurrentDataLoader(fed,
//              jobState.nthreads, jobState.queueCapacity,
//              jobState.rejectedExecutionDelay, jobState.maxTries);

        final AsynchronousStatementBufferFactory<BigdataStatement> statementBufferFactory = new AsynchronousStatementBufferFactory<BigdataStatement>(
                (ScaleOutTripleStore) tripleStore,//
                jobState.asynchronousWritesProducerChunkSize,//
                jobState.valuesInitialCapacity,//
                jobState.bnodesInitialCapacity,//
                RDFFormat.RDFXML, // defaultFormat
                false, // verifyData
                false, // deleteAfter
                jobState.nthreads, // parserPoolSize 
                jobState.queueCapacity, // parserQueueCapacity
                jobState.term2IdWriterPoolSize,//
                jobState.otherWriterPoolSize,//
                jobState.unbufferedStatementThreshold
                );

        try {

            /*
             * Note: Add the counters to be reported to the client's counter
             * set. The added counters will be reported when the client reports
             * its own counters.
             */
            final CounterSet serviceRoot = fed.getServiceCounterSet();

            final String relPath = jobState.jobName + ICounterSet.pathSeparator
                    + "Concurrent Data Loader";

            synchronized (serviceRoot) {

                if (serviceRoot.getPath(relPath) == null) {

                    // Create path to CDL counter set.
                    final CounterSet tmp = serviceRoot.makePath(relPath);

//                    // Attach CDL counters.
//                    tmp.attach(loader.getCounters());
//
//                    // Attach task factory counters.
//                    tmp.attach(taskFactory.getCounters());

                    tmp.attach(statementBufferFactory.getCounters());
                    
                }

            }

//            // Let the loader know that we will run tasks.
//            taskFactory.notifyStart();
//
//            // Load data.
//            loadData(loader, taskFactory, tripleStore);
//
//            // Done loading data (stops the clock).
//            taskFactory.notifyEnd();
//
//            // Shutdown the loader.
//            loader.shutdown();

            loadData(statementBufferFactory);
            
            if (statementBufferFactory != null) {
                
                if (log.isInfoEnabled())
                    log.info("Closing async writers.");
                
                statementBufferFactory.awaitAll();
                
            }

        } catch (Throwable t) {

            log.error("Task failed: " + t, t);
            
            try {
//                loader.shutdownNow();
                if (statementBufferFactory != null) {
                    statementBufferFactory
                            .cancelAll(true/* mayInterruptIfRunning */);
                }
            } catch (Throwable t2) {
                log.warn(this, t2);
            }

            throw new RuntimeException(t);

        }

        return null;

    }

    /**
     * Hook to load data is invoked once by {@link #call()}. 
     * 
     * @throws InterruptedException
     *             if interrupted (job canceled).
     * @throws Exception
     *             if something else goes wrong.
     */
    protected void loadData(//
            final AsynchronousStatementBufferFactory<BigdataStatement> factory)
            throws InterruptedException, Exception {

        factory.submitAll(jobState.dataDir, jobState.dataDirFilter);

    }

//    /**
//     * Hook to load data is invoked once by {@link #call()}. The default
//     * implementation uses {@link #loadDataFromFiles(AbstractTripleStore)}
//     * to load any files found in the {@link JobState#dataDir}.
//     * 
//     * @param tripleStore
//     *            Where to put the data.
//     * 
//     * @throws InterruptedException
//     *             if interrupted (job canceled).
//     * @throws Exception
//     *             if something else goes wrong.
//     */
//    protected void loadData(//
//            final ConcurrentDataLoader loader,//
//            final AbstractRDFTaskFactory taskFactory,//
//            final AbstractTripleStore tripleStore//
//    ) throws InterruptedException, Exception {
//
//        loadDataFromFiles(loader, taskFactory, tripleStore);
//
//    }
//
//
//    /**
//     * Runs a {@link RunnableFileSystemLoader} to loads data from the
//     * {@link JobState#dataDir} and optionally deletes files after they have
//     * been loaded successfully.
//     * 
//     * @throws InterruptedException
//     * @throws Exception
//     */
//    protected void loadDataFromFiles(final ConcurrentDataLoader loader,
//            final AbstractRDFTaskFactory taskFactory,
//            final AbstractTripleStore tripleStore) throws InterruptedException,
//            Exception {
//
//        new RunnableFileSystemLoader(loader, taskFactory, tripleStore,
//                jobState.dataDir, jobState.dataDirFilter).run();
//        
//    }

//    /**
//     * {@link Runnable} class applies the factory to either a single file or to
//     * all files within a directory.
//     */
//    static protected class RunnableFileSystemLoader implements Runnable {
//
//        final protected transient static Logger log = Logger
//                .getLogger(RunnableFileSystemLoader.class);
//
//        volatile boolean done = false;
//
//        final ConcurrentDataLoader loader;
//
//        final AbstractRDFTaskFactory taskFactory;
//
//        final AbstractTripleStore tripleStore;
//
//        final File fileOrDir;
//        
//        final FilenameFilter filter;
//
//        /**
//         * 
//         * @param loader
//         * @param taskFactory
//         * @param tripleStore
//         * @param fileOrDir
//         *            The file or directory to be loaded.
//         * @param filter
//         *            An optional filter on files that will be accepted when
//         *            processing a directory.
//         */
//        public RunnableFileSystemLoader(final ConcurrentDataLoader loader,
//                final AbstractRDFTaskFactory taskFactory,
//                final AbstractTripleStore tripleStore, final File fileOrDir,
//                final FilenameFilter filter) {
//
//            if (loader == null)
//                throw new IllegalArgumentException();
//            if (taskFactory == null)
//                throw new IllegalArgumentException();
//            if (tripleStore == null)
//                throw new IllegalArgumentException();
//            if (fileOrDir == null)
//                throw new IllegalArgumentException();
//
//            this.loader = loader;
//
//            this.taskFactory = taskFactory;
//
//            this.tripleStore = tripleStore;
//
//            this.fileOrDir = fileOrDir;
//
//            this.filter = filter; // MAY be null.
//            
//        }
//
//        /**
//         * Creates a task using the {@link #taskFactory}, submits it to the
//         * {@link #loader} and and waits for the task to complete.
//         * <p>
//         * Errors are logged, but not thrown. This makes the task suitable
//         * for use with a {@link ScheduledExecutorService}.
//         * 
//         * @throws RuntimeException
//         *             if interrupted.
//         */
//        public void run() {
//
//            try {
//
//                if (log.isInfoEnabled())
//                    log.info("start: file=" + fileOrDir);
//
//                // loads everything in the file or directory.
//                process(fileOrDir);
//
//                // wait until the data have been loaded.
//                loader.awaitCompletion(Long.MAX_VALUE, TimeUnit.SECONDS);
//
//                if (log.isInfoEnabled())
//                    log.info("done : file=" + fileOrDir);
//
//            } catch (InterruptedException t) {
//
//                // Note: This is normal termination.
//                log.warn("Interrupted");
//
//                throw new RuntimeException("Interrupted.");
//
//            } catch (Throwable t) {
//
//                // Catch and log all errors.
//                log.error(fileOrDir, t);
//
//            }
//
//        }
//
//        /**
//         * Scans file(s) recursively starting with the named file, creates a
//         * task using the {@link ITaskFactory} for each file that passes the
//         * filter, and submits the task. When <i>file</i> is a directory, the
//         * method returns once all file(s) in the directory and its children
//         * have been submitted for processing.
//         * 
//         * @param file
//         *            Either a plain file or directory containing files to be
//         *            processed.
//         * @param filter
//         *            An optional filter.
//         * @param taskFactory
//         * 
//         * @throws InterruptedException
//         *             if the thread is interrupted while queuing tasks.
//         */
//        public void process(final File file) throws InterruptedException {
//
//            if (file == null)
//                throw new IllegalArgumentException();
//
//            if (taskFactory == null)
//                throw new IllegalArgumentException();
//
//            process2(file, filter, taskFactory);
//
//        }
//        
//        private void process2(final File file, final FilenameFilter filter,
//                final ITaskFactory taskFactory) throws InterruptedException {
//
//            if (file.isDirectory()) {
//
//                if (log.isInfoEnabled())
//                    log.info("Scanning directory: " + file);
//
//                // filter is optional.
//                final File[] files = filter == null ? file.listFiles() : file
//                        .listFiles(filter);
//
//                for (final File f : files) {
//
//                    process2(f, filter, taskFactory);
//
//                }
//
//            } else {
//
//                /*
//                 * Processing a standard file.
//                 */
//
//                if (log.isInfoEnabled())
//                    log.info("Scanning file: " + file);
//
//                try {
//
//                    loader.submitTask(file.getPath(), taskFactory);
//                    
//                } catch (Exception ex) {
//                    
//                    log.error(file, ex);
//                    
//                }
//
//            }
//
//        }
//
//    } // RunnableFileSystemLoader

}
