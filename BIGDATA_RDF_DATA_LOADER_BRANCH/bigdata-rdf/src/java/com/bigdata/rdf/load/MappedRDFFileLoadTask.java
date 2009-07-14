package com.bigdata.rdf.load;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.counters.CounterSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.load.MappedRDFDataLoadMaster.JobState;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.rio.AsynchronousStatementBufferFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.master.AbstractAsynchronousClientTask;
import com.bigdata.service.jini.master.ClientLocator;
import com.bigdata.service.jini.master.INotifyOutcome;

/**
 * Task reads files from the file system, loads them into an
 * {@link ITripleStore}, and optionally deletes the source files once they are
 * restart safe on the database. This is a non-transactional bulk load using
 * unisolated writes. This task may be used in conjunction with any process that
 * writes files into a known directory on the hosts of a cluster.
 * <p>
 * Note: Counters reporting the progress of this task will be attached to the
 * service within which this task is executing.
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
public class MappedRDFFileLoadTask<//
S extends JobState,//
V extends Serializable,//
L extends ClientLocator//
> extends AbstractAsynchronousClientTask<Void,V,L>//
implements Serializable {

    final protected transient static Logger log = Logger
            .getLogger(MappedRDFFileLoadTask.class);

    /**
     * 
     */
    private static final long serialVersionUID = 6787939197771556658L;

    protected final S jobState;

    protected final L locator;
    
    /**
     * Instantiated by {@link #call()} on the {@link IRemoteExecutor} service.
     */
    private transient AsynchronousStatementBufferFactory<BigdataStatement> statementBufferFactory;

    /**
     * Note: transient field set by {@link #call()}. 
     */
    private transient ReentrantLock lock;
    /**
     * Note: transient field set by {@link #call()}.
     */
    private transient Condition allDone;
    
    public String toString() {

        return getClass().getName() + "{clientNum=" + locator + "}";

    }

    public MappedRDFFileLoadTask(final S jobState,
            final INotifyOutcome<V, L> notifyProxy, final L locator) {

        super(notifyProxy);

        if (locator == null)
            throw new IllegalArgumentException();

        this.jobState = jobState;

        this.locator = locator;

    }

    /**
     * The federation object used by the {@link IRemoteExecutor} on which this
     * task is executing.
     */
    public JiniFederation getFederation() {

        return (JiniFederation) super.getFederation();

    }

    public Void call() throws Exception {

        // set transient fields.
        lock = new ReentrantLock();
        allDone = lock.newCondition();
        
        final JiniFederation fed = getFederation();

        final AbstractTripleStore tripleStore = (AbstractTripleStore) fed
                .getResourceLocator()
                .locate(jobState.namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

            throw new RuntimeException("KB not found: namespace="
                    + jobState.namespace);

        }

        statementBufferFactory = new AsynchronousStatementBufferFactory<BigdataStatement>(
                (ScaleOutTripleStore) tripleStore,//
                jobState.producerChunkSize,//
                jobState.valuesInitialCapacity,//
                jobState.bnodesInitialCapacity,//
                RDFFormat.RDFXML, // @todo defaultFormat is not serializable.
                jobState.parserValidates,//
                jobState.deleteAfter,//
                jobState.parserPoolSize, // parserPoolSize 
                jobState.parserQueueCapacity, // parserQueueCapacity
                jobState.term2IdWriterPoolSize,//
                jobState.otherWriterPoolSize,//
                jobState.unbufferedStatementThreshold
                ) {
                        
            /*
             * Override the "notifyService" to do asynchronous RMI back to this
             * class indicating success or failure for each resource.
             * 
             * @todo raise notifyServicePoolSize into ctor and specify GTE ONE
             * (1) since concurrent RMI could speed up asynchronous notification
             * sigificantly.
             */
            protected Runnable newSuccessTask(final String resource) {
                return new Runnable() {
                    public void run() {
                        try {
                            getNotifyProxy().success(resource, locator);
                        } catch (RemoteException ex) {
                            log.error(resource, ex);
                        }
                    }
                };
            }

            protected Runnable newFailureTask(final String resource,
                    final Throwable cause) {
                return new Runnable() {
                    public void run() {
                        try {
                            getNotifyProxy().error(resource, locator, cause);
                        } catch (RemoteException ex) {
                            log.error(resource, ex);
                        }
                    }
                };
            }

        };

        try {

            /*
             * Note: Add the counters to be reported to the client's counter
             * set. The added counters will be reported when the client reports
             * its own counters.
             */
            final CounterSet serviceRoot = fed.getServiceCounterSet();

            final String relPath = jobState.jobName;

            // Create path to CDL counter set.
            final CounterSet tmp = serviceRoot.makePath(relPath);

            tmp.attach(statementBufferFactory.getCounters(), true/* replace */);

            /*
             * Wait until either (a) interrupted by the master using
             * Future#cancel(); or (b) the master invokes close(), indicating
             * that no more work items will be tasked to the client.
             */
            lock.lockInterruptibly();
            try {
                try {
                    allDone.await();
                } catch (InterruptedException ex) {
                    if (log.isInfoEnabled())
                        log.info("Client will terminate.");
                }
            } finally {
                lock.unlock();
            }

        } finally {

            try {
                statementBufferFactory
                        .cancelAll(true/* mayInterruptIfRunning */);
            } catch (Throwable t2) {
                log.warn(this, t2);
            }

        }

        return null;

    }

    /**
     * FIXME This knows how to open {@link File}s and {@link URL}s, but it is
     * not integrated into the {@link AsynchronousStatementBufferFactory} and
     * the factory is parameterized for {@link String} resource identifiers, so
     * the identifier would need to be converted to a {@link File}, {@link URL},
     * etc. and each possibility tested.
     */
    @Override
    protected InputStream openResource(final V resource) throws IOException {

        final InputStream is;

        if (resource instanceof File) {

            is = new FileInputStream((File) resource);

        } else if (resource instanceof URL) {

            is = ((URL) resource).openStream();

        } else {

            throw new UnsupportedOperationException();

        }

        return new BufferedInputStream(is);

    }

    public void accept(V[] chunk) throws RemoteException, InterruptedException {

        for (V resource : chunk) {

            statementBufferFactory.submitOne(resource.toString(),
                    jobState.rejectedExecutionDelay);

        }

    }

    public void close() throws RemoteException, InterruptedException {

        lock.lockInterruptibly();
        try {

            statementBufferFactory.awaitAll();

            if (log.isInfoEnabled())
                log.info("Done.");

            /*
             * Signal in case master did not interrupt the main thread in
             * call().
             */
            allDone.signal();
            
        } catch (ExecutionException ex) {

            throw new RuntimeException(ex);
            
        } finally {

            lock.unlock();
            
        }

    }

}
