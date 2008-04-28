/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Oct 10, 2007
 */

package com.bigdata.journal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.concurrent.LockManager;
import com.bigdata.concurrent.LockManagerTask;
import com.bigdata.counters.CounterSet;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.util.InnerCause;

/**
 * Abstract base class for tasks that may be submitted to the
 * {@link ConcurrencyManager}. Tasks may be isolated (by a transaction),
 * unisolated, read-committed, or historical reads. Tasks access named resources
 * (aka indices), which they pre-declare in their constructors.
 * <p>
 * A read-committed task runs against the most recently committed view of the
 * named index. A historical read task runs against a historical view of the
 * named index, but without guarentees of transactional isolation. Concurrent
 * readers are permitted without locking on the same index.
 * <p>
 * An unisolated task reads and writes on the "live" index. Note that only a
 * single thread may write on a {@link BTree} at a time. Therefore unisolated
 * tasks (often referred to as writers) obtain an exclusive lock on the named
 * index(s). When more than one named index is used, the locks are used to infer
 * a partial ordering of the writers allowing as much concurrency as possible.
 * Pre-declaration of locks allows us to avoid deadlocks in the lock system.
 * <p>
 * Isolated tasks are part of a larger transaction. Transactions are started and
 * committed using an {@link ITransactionManagerService}. Transactional tasks
 * run with full concurrency using an MVCC (Multi-Version Concurrency Control)
 * strategy. When a transaction is committed (by the
 * {@link ITransactionManagerService}) it must wait for lock(s) on the
 * unisolated named indices on which it has written before it may validate and
 * commit.
 * <p>
 * Note: You MUST submit a distinct instance of this task each time you
 * {@link ConcurrencyManager#submit(AbstractTask)} it.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractTask implements Callable<Object>, ITask {

    static protected final Logger log = Logger.getLogger(AbstractTask.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    static final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    static final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Used to protect against re-submission of the same task object.
     */
    private final AtomicBoolean submitted = new AtomicBoolean(false);

    /**
     * The object used to manage exclusive access to the unisolated indices.
     */
    protected final ConcurrencyManager concurrencyManager;
    
    /**
     * The object used to manage local transactions.
     */
    protected final ILocalTransactionManager transactionManager;
    
    /**
     * The object used to manage access to the resources from which views of the
     * indices are created.
     */
    protected final IResourceManager resourceManager;
    
    /**
     * The object used to manage access to the resources from which views of the
     * indices are created.
     */
    public final IResourceManager getResourceManager() {
        
        return resourceManager;
        
    }
    
    synchronized public final IJournal getJournal() {

        if (journal == null) {

            journal = resourceManager.getJournal(Math.abs(timestamp));

            if (journal == null) {

                log.warn("No such journal: timestamp=" + timestamp);

                return null;
                
            }
            
            if (readOnly) {

                // disallow writes.
                journal = new ReadOnlyJournal(journal);
                
            }

        }

        return journal;

    }

    private IJournal journal;
    
    /**
     * The transaction identifier -or- {@link ITx#UNISOLATED} IFF the operation
     * is NOT isolated by a transaction -or- <code> - timestamp </code> to read
     * from the most recent commit point not later than the absolute value of
     * <i>timestamp</i>.
     */
    protected final long timestamp;

    /**
     * True iff the operation is isolated by a transaction.
     */
    protected final boolean isTransaction;

    /**
     * True iff the operation is not permitted to write.
     */
    protected final boolean readOnly;

    /**
     * The name of the resource(s) on which the task will operation (read,
     * write, add, drop, etc). This is typically the name of one or more
     * indices.
     * <p>
     * Note: When the operation is an unisolated writer an exclusive lock MUST
     * be obtain on the named resources(s) before the operation may execute.
     * <p>
     * Note: this is private so that people can not mess with the resource names
     * in {@link #doTask()}.
     */
    private final String[] resource;

    /**
     * The transaction object iff the operation is isolated by a transaction
     * and otherwise <code>null</code>. 
     */
    protected final ITx tx;
    
    /**
     * Cache of resolved named indices.
     * 
     * @see #getIndex(String name)
     */
    final private Map<String,IIndex> indexCache;

    /**
     * Flag is cleared if the task is aborted.  This is used to refuse
     * access to resources for tasks that ignore interrupts.
     */
    boolean aborted = false;
    
    /**
     * The {@link AbstractTask} increments various counters of interest to the
     * {@link ConcurrencyManager} using this object.
     */
    final TaskCounters counters;
    
    /*
     * Timing data for this task.
     */
    
    /**
     * The time at which this task was submitted to the {@link ConcurrencyManager}.
     */
    public long nanoTime_submitTask;
    
    /**
     * The time at which this task was assigned to a worker thread for
     * execution.
     */
    public long nanoTime_assignedWorker;

    /**
     * The time at which this task began to do its work. If the task needs to
     * acquire exclusive resource locks, then this timestamp is set once those
     * locks have been acquired. Otherwise this timestamp will be very close to
     * the {@link #nanoTime_assignedWorker}.
     */
    public long nanoTime_beginWork;
    
    /**
     * The time at which this task finished its work. Tasks with write sets must
     * still do abort processing or await the next commit group.
     */
    public long nanoTime_finishedWork;
    
    /**
     * The time at which the task was done. 
     */
    public long nanoTime_allDone;
    
    /**
     * Convenience constructor variant for one named resource.
     * 
     * @param concurrencyControl
     *            The object used to control access to the local resources.
     * @param timestamp
     *            The transaction identifier -or- {@link ITx#UNISOLATED} IFF the
     *            operation is NOT isolated by a transaction -or-
     *            <code> - timestamp </code> to read from the most recent commit
     *            point not later than the absolute value of <i>timestamp</i>
     *            (a historical read).
     * @param resource
     *            The resource on which the task will operate. E.g., the names
     *            of the index. When the task is an unisolated write task an
     *            exclusive lock will be requested on the named resource and the
     *            task will NOT run until it has obtained that lock.
     */
    protected AbstractTask(IConcurrencyManager concurrencyManager,
            long timestamp, String resource) {

        this(concurrencyManager, timestamp, new String[] { resource });
        
    }
    
    /**
     * 
     * @param concurrencyControl
     *            The object used to control access to the local resources.
     * @param timestamp
     *            The transaction identifier -or- {@link ITx#UNISOLATED} IFF the
     *            operation is NOT isolated by a transaction -or-
     *            <code> - tx </code> to read from the most recent commit point
     *            not later than the absolute value of <i>tx</i> (a fully
     *            isolated read-only transaction using a historical start time).
     * @param resource
     *            The resource(s) on which the task will operate. E.g., the
     *            names of the index(s). When the task is an unisolated write
     *            task an exclusive lock will be requested on each named
     *            resource and the task will NOT run until it has obtained those
     *            lock(s).
     */
    protected AbstractTask(IConcurrencyManager concurrencyManager,
            long timestamp, String[] resource) {
        
        if (concurrencyManager == null) {

            throw new NullPointerException();

        }
        
        if (resource == null) {

            throw new NullPointerException();
            
        }

        for(int i=0; i<resource.length; i++) {
            
            if (resource[i] == null) {

                throw new NullPointerException();
                
            }
            
        }

        // make sure we have the real ConcurrencyManager for addCounters()
        this.concurrencyManager = (ConcurrencyManager) (concurrencyManager instanceof Journal ? ((Journal) concurrencyManager)
                .getConcurrencyManager()
                : concurrencyManager);

        this.transactionManager = concurrencyManager.getTransactionManager();

        this.resourceManager = concurrencyManager.getResourceManager();
        
        this.timestamp = timestamp;

        this.isTransaction = timestamp > ITx.UNISOLATED;
        
        this.resource = resource;

        this.indexCache = new HashMap<String,IIndex>(resource.length);
        
        if (isTransaction) {

            /*
             * A transaction.
             * 
             * Transactions are always identified by a positive timestamp giving
             * the startTime of the transaction and from which we can determine
             * the commit record from which that transaction will read.
             * 
             * The transaction startTime is generated by the transaction
             * manager. When the desired startTime is historical, the
             * transaction manager will (a) verify that it can assert read locks
             * for the historical commit point; and (b) return a startTime not
             * in use by any other transaction which will be used to identify
             * the new transaction - if necessary the request for the new
             * transaction may be delayed until a suitable startTime is
             * available.
             * 
             * If the transaction is read-write then it will write on a fully
             * isolated write set.
             */

            tx = transactionManager.getTx(timestamp);

            if (tx == null) {

                throw new IllegalStateException("Unknown tx: "+timestamp);

            }

            if (!tx.isActive()) {

                throw new IllegalStateException("Tx not active: "+timestamp);

            }

            readOnly = tx.isReadOnly();
            
            counters = this.concurrencyManager.countersTX;
            
        } else if (timestamp < ITx.UNISOLATED) {

            /*
             * A lightweight historical read.
             * 
             * Note: A lightweight historical reads does not assert read locks
             * and is not coordinated with the transaction manager.
             * 
             * Note: Read-committed isolation is handled as a special case of
             * lightweight historical read.
             */

            tx = null;
            
            readOnly = true;

            counters = this.concurrencyManager.countersHR;
            
        } else {

            /*
             * Unisolated operation.
             */

            tx = null;
            
            readOnly = false;

            counters = this.concurrencyManager.countersUN;
            
        }

    }

    /**
     * Returns a copy of the array of resources declared to the constructor.
     */
    public String[] getResource() {

        // clone so that people can not mess with the resource names.
        return resource.clone();

    }

    /**
     * Return the only declared resource.
     * 
     * @return The declared resource.
     * 
     * @exception IllegalStateException
     *                if more than one resource was declared.
     */
    public String getOnlyResource() {
        
        if (resource.length > 1)
            throw new IllegalStateException("More than one resource was declared");
        
        return resource[0];
        
    }
    
    /**
     * Returns Task{taskName,timestamp,resource[]}
     */
    public String toString() {
        
        return "Task{"+getTaskName()+",timestamp="+timestamp+",resource="+Arrays.toString(resource)+"}";
        
    }
    
    /**
     * Returns the name of the class by default.
     */
    protected String getTaskName() {
        
        return getClass().getName();
        
    }
    
    /**
     * Asserts that the <i>resource</i> is one of the resource(s) declared to
     * the constructor. This is used to prevent tasks from accessing resources
     * that they did not declare (and on which they may not hold a lock).
     * 
     * @param resource
     *            A resource name.
     * 
     * @return The resource name.
     * 
     * @exception IllegalStateException
     *                if the <i>resource</i> was not declared to the
     *                constructor.
     */
    protected String assertResource(String resource) {
        
        final String[] a = this.resource;
        
        for(int i=0; i<a.length; i++) {
            
            if(a[i].equals(resource)) {
                
                return resource;
                
            }
            
        }
        
        throw new IllegalStateException("Not declared by task: "+resource);
        
    }
    
    /**
     * Implement the task behavior here.
     * <p>
     * Note: Long-running implementations MUST periodically test
     * {@link Thread#interrupted()} and MUST throw an exception, such as
     * {@link InterruptedException}, if they are interrupted. This behavior
     * allows tasks to be cancelled in a timely manner.
     * <p>
     * If you ignore or fail to test {@link Thread#interrupted()} then your task
     * CAN NOT be aborted. If it is {@link Future#cancel(boolean)} with
     * <code>false</code> then the task will run to completion even though it
     * has been cancelled (but the {@link Future} will appear to have been
     * cancelled).
     * <p>
     * If you simply <code>return</code> rather than throwing an exception
     * then the {@link WriteExecutorService} will assume that your task
     * completed and your (partial) results will be made restart-safe at the
     * next commit!
     * 
     * @return The object that will be returned by {@link #call()} iff the
     *         operation succeeds.
     * 
     * @throws Exception
     *             The exception that will be thrown by {@link #call()} iff the
     *             operation fails.
     * 
     * @exception InterruptedException
     *                This exception SHOULD be thrown if
     *                {@link Thread#interrupted()} becomes true during
     *                execution.
     */
    abstract protected Object doTask() throws Exception;

    /**
     * Adds the following fields to the {@link MDC} logging context:
     * <dl>
     * <dt>taskname</dt>
     * <dd>The name of the task as reported by {@link #getTaskName()}.</dd>
     * <dt>timestamp</dt>
     * <dd>The {@link #timestamp} specified to the ctor.</dd>
     * <dt>resources</dt>
     * <dd>The named resource(s) specified to the ctor.</dd>
     * </dl>
     */
    protected void setupLoggingContext() {

        // Add to the logging context for the current thread.
            
        MDC.put("taskname", getTaskName());

        MDC.put("timestamp", ""+timestamp);
        
        MDC.put("resources", Arrays.toString(resource));
        
    }

    /**
     * Clear fields set by {@link #setupLoggingContext()} from the {@link MDC}
     * logging context.
     */
    protected void clearLoggingContext() {

        MDC.remove("taskname");

        MDC.remove("timestamp");

        MDC.remove("resources");
        
    }
    
    /**
     * Delegates the task behavior to {@link #doTask()}.
     * <p>
     * For an unisolated operation, this method provides safe commit iff the
     * task succeeds and otherwise invokes abort() so that partial task
     * executions are properly discarded. When possible, the original exception
     * is re-thrown so that we do not encapsulate the cause unless it would
     * violate our throws clause.
     * <p>
     * Commit and abort are NOT invoked for an isolated operation regardless of
     * whether the operation succeeds or fails. It is the responsibility of the
     * "client" to commit or abort a transaction as it sees fit.
     * <p>
     * Note: Exceptions that are thrown from here will be wrapped as
     * {@link ExecutionException}s by the {@link ExecutorService}. Use
     * {@link InnerCause} to test for these exceptions.
     * 
     * @throws StaleLocatorException
     *             if the task requests an index partition which has been split,
     *             joined, or moved to another data service.
     * @throws NoSuchIndexException
     *             if the task requests an index that is not registered on the
     *             data service.
     * @throws InterruptedException
     *             can be thrown if the task is interrupted, for example while
     *             awaiting a lock, if the commit group is being discarded, or
     *             if the journal is being shutdown (which will cause the
     *             executor service running the task to be shutdown and thereby
     *             interrupt all running tasks).
     */
    final public Object call() throws Exception {
        
        try {

            /*
             * Increment by the amount of time that the task waited on the queue
             * before it began to execute.
             */
            
            final long waitingTime = (System.nanoTime() - nanoTime_submitTask);
            
            counters.queueWaitingTime += waitingTime;
            
            setupLoggingContext();
            
            final Object ret = call2();
            
            clearLoggingContext();

            counters.taskSuccessCount++;

            return ret;

        } catch(Exception e) {
            
            /*
             * Note: covers RuntimeException, just not Throwable and the Error
             * hierarchy (which you are not supposed to catch).
             */
            
            counters.tailFailCount++;
            
            throw e;
            
        } finally {

            counters.taskCompleteCount++;
            
            // increment by the amount of time that the task was executing.
            counters.serviceNanoTime += (nanoTime_finishedWork - nanoTime_beginWork);

            // increment by the total time from submit to completion.
            counters.queuingNanoTime += (nanoTime_finishedWork - nanoTime_submitTask);
            
        }

    }

    final private Object call2() throws Exception {

        nanoTime_assignedWorker = System.nanoTime();

        try {
            
            if (!submitted.compareAndSet(false, true)) {

                throw new ResubmitException(toString());
                
            }
            
            if (isTransaction) {

                log.info("Running isolated operation: timestamp="+timestamp);
                
                if(tx.isReadOnly()) {

                    try {

                        nanoTime_beginWork = System.nanoTime();
                        
                        return doTask();

                    } finally {

                        nanoTime_finishedWork = System.nanoTime();
                        
                        // release hard references to named read-only indices.
                        
                        clearIndexCache();
                        
                    }

                }
                
                /*
                 * Delegate handles handshaking for writable transactions.
                 */

                final Callable<Object> delegate = new InnerReadWriteTxServiceCallable(
                        this, tx);
                
                return delegate.call();

            }

            if (readOnly) {

                try {

                    nanoTime_beginWork = System.nanoTime();
                    
                    return doTask();
                    
                } finally {
                    
                    nanoTime_finishedWork = System.nanoTime();

                    // release hard references to the named read-only indices.
                    
                    clearIndexCache();
                
                    log.info("Reader is done: "+this);
                    
                }

            } else {

                /*
                 * Handle unisolated write tasks, which need to coordinate with
                 * the lock manager for the unisolated indices.
                 */

                assert timestamp == ITx.UNISOLATED;
                
                return doUnisolatedReadWriteTask();
                
            }

        } finally {

            nanoTime_allDone = System.currentTimeMillis();
            
            log.info("done: "+this);

        }

    }
    
    /**
     * Call {@link #doTask()} for an unisolated write task.
     * <p>
     * Note: This coordinates with {@link IConcurrencyManager#getLockManager()}
     * to force a schedule on tasks that write on unisolated indices.
     * 
     * @throws Exception
     */
    private Object doUnisolatedReadWriteTask() throws Exception {
        
        // lock manager.
        final LockManager<String> lockManager = concurrencyManager.getLockManager();

        // resource(s) to lock (exclusive locks are used).

        log.info("Unisolated write task: "+this+", thread="+Thread.currentThread());

        // declare resource(s).
        lockManager.addResource(resource);

        // delegate will handle lock acquisition and invoke doTask().
        LockManagerTask<String> delegate = new LockManagerTask<String>(lockManager,
                resource, new InnerWriteServiceCallable(this));
        
        final WriteExecutorService writeService = concurrencyManager.getWriteService();

        writeService.beforeTask(Thread.currentThread(), this);

        boolean ran = false;

        try {

            final Object ret;
            
            /*
             * Note: The lock(s) are only held during this call. By the time the
             * call returns any lock(s) have been released. Locks MUST be
             * released as soon as the task is done writing so that it does NOT
             * hold locks while it is awaiting commit. This make it possible for
             * other operations to write on the same index in the same commit
             * group.
             */

            try {

                ret = delegate.call();
                
            } finally {
                
                /*
                 * Increment by the amount of time that the task was waiting to
                 * acquire its lock(s).
                 */

                counters.lockWaitingTime += delegate.getLockLatency();
                
            }

            if (Thread.interrupted()) {

                throw new InterruptedException();

            }

            // set flag.
            ran = true;

            log.info("Task Ok: class=" + this);
                      
            /*
             * Note: The WriteServiceExecutor will await a commit signal before
             * the thread is allowed to complete. This ensures that the caller
             * waits until a commit (or an abort).
             * 
             * Note: Waiting here does NOT prevent other tasks from gaining
             * access to the same resources since the locks were released above.
             */ 

            writeService.afterTask(this, null);

            return ret;

        } catch (Throwable t) {

            if (!ran) {

                // Do not re-invoke it afterTask failed above.

                log.info("Task failed: class="+this+" : "+t);
                
                writeService.afterTask(this, t);

            }

            /*
             * Throw whatever exception was thrown by the task (or by afterTask
             * if it craps out).
             */
            
            if (t instanceof Exception)
                throw (Exception) t;

            throw new RuntimeException(t);

        } finally {
            
            // discard hard references to accessed indices.
            clearIndexCache();
            
        }

    }

    /**
     * Delegates various behaviors visible to the application code using the
     * {@link ITask} interface to the {@link AbstractTask} object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static abstract protected class DelegateTask implements ITask {
        
        final protected AbstractTask delegate;
        
        protected DelegateTask(AbstractTask delegate) {
            
            if(delegate == null) throw new IllegalArgumentException(); 
            
            this.delegate = delegate;
            
        }

        public IResourceManager getResourceManager() {

            return delegate.getResourceManager();
        
        }

        public IJournal getJournal() {

            return delegate.getJournal();
            
        }

        public String[] getResource() {

            return delegate.getResource();
            
        }

        public String getOnlyResource() {

            return delegate.getOnlyResource();
            
        }

        public IIndex getIndex(String name) {

            return delegate.getIndex(name);
            
        }
                
        public String toString() {
            
            return getClass().getName()+"("+delegate.toString()+")";
            
        }
        
    }
    
    /**
     * Inner class used to wrap up the call to {@link AbstractTask#doTask()} for
     * {@link IsolationEnum#ReadWrite} transactions.
     * 
     * FIXME take note of which indices the transaction actually _writes_ on and
     * then inform the transaction manager which needs to keep track of that
     * information. Notify the transaction manager in the post-processing for
     * each isolated transaction, but only if a new index is introduced into the
     * write set of the transaction.
     * 
     * FIXME In order to allow concurrent tasks to do work on the same
     * transaction we need to use a per-transaction {@link LockManager} to
     * produce a partial order that governs access to the isolated (vs mutable
     * unisolated) indices accessed by that transaction for the
     * <strong>txService</strong>.
     * <p>
     * Make sure that the {@link TemporaryRawStore} supports an appropriate
     * level of concurrency to allow concurrent writers on distinct isolated
     * indices that are being buffered on that store for a given transaction
     * (MRMW). Reads and writes are currently serialized in order to support
     * overflow from memory to disk.
     * <p>
     * The {@link Tx} needs to be thread-safe when instantiating the temporary
     * store and when granting a view of an index (the read-committed and
     * read-only txs should also be thread safe in this regard).
     * <p>
     * The txService must be a {@link WriteExecutorService} so that it will
     * correctly handle aborts and commits of writes on isolated indices.
     * 
     * FIXME The transaction write service needs to be paused during synchronous
     * overflow handling and buffered transaction write sets need to be split
     * and moved with their index partitions since validation depends on an
     * unisolated operation comparing the local write set for an index partition
     * with the local index partition.
     */
    static protected class InnerReadWriteTxServiceCallable extends DelegateTask {

        final ITx tx;
        
        InnerReadWriteTxServiceCallable(AbstractTask delegate, ITx tx) {
            
            super( delegate );
            
            if (tx == null)
                throw new IllegalArgumentException();
            
            this.tx = tx;
            
        }

        /**
         * Wraps up the execution of {@link AbstractTask#doTask()}.
         */
        public Object call() throws Exception {

            // invoke on the outer class.

            try {

                delegate.nanoTime_beginWork = System.nanoTime();
                
                return delegate.doTask();
                
            } finally {

                delegate.nanoTime_finishedWork = System.nanoTime();

                /*
                 * Release hard references to the named indices. The backing
                 * indices are reading from the ground state identified by the
                 * start time of the ReadWrite transaction.
                 */
                
                delegate.clearIndexCache();
                
            }
            
        }

    }
    
    /**
     * An instance of this class is used as the delegate for a
     * {@link LockManagerTask} in order to coordinate the acquisition of locks
     * with the {@link LockManager} before the task can execute and to release
     * locks after the task has completed (whether it succeeds or fails).
     * <p>
     * Note: This inner class delegates the execution of the task to
     * {@link AbstractTask#doTask()} on the outer class.
     * <p>
     * Note: If there is only a single writer thread then the lock system
     * essentially does nothing. When there are multiple writer threads the lock
     * system imposes a partial ordering on the writers that ensures that writes
     * on a given named index are single-threaded and that deadlocks do not
     * prevent tasks from progressing. If there is strong lock contention then
     * writers will be more or less serialized.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class InnerWriteServiceCallable extends DelegateTask {

        InnerWriteServiceCallable(AbstractTask delegate) {
            
            super(delegate);
            
        }

        /**
         * Note: Locks on the named indices are ONLY held during this call.
         */
        public Object call() throws Exception {

            delegate.nanoTime_beginWork = System.nanoTime();

            // The write service on which this task is running.
            final WriteExecutorService writeService = delegate.concurrencyManager.getWriteService();

            /*
             * Get reference to lock (this is just a reference - we acquire the
             * lock itself below).
             * 
             * Note: this object is used to ensure that checkpoints and
             * rollbacks are coordinated with the write service. In particular,
             * this ensures that checkpoint and rollback operations do NOT
             * overlap a group commit. Since indices do not participate in group
             * commits until we write their checkpoint records and since we do
             * not write their checkpoint records until we have this lock, we
             * are able to guarentee that the write set of a task is made
             * restart safe atomically by the next group commit.
             * 
             * FIXME NOW : when the task succeeds, it should be possible to flush the
             * index to the store before we obtain the lock in order to reduce
             * the latency when we write the checkpoint record. (if the task
             * fails, the rollback is a light weight operation.)
             * 
             * FIXME NOW : In fact, the checkpoint and rollback actions then become
             * simply recording the new checkpoint record. for checkpoint, it
             * has the new root address and (if changed) the new metadata
             * address. for rollback, we simply close the index and write a copy
             * of the saved checkpoint record, thereby discarding any possible
             * side effects from checkpoints performed from within the task
             * itself.
             */
            final Lock lock = writeService.lock;

            try {

                if(INFO)
                log.info("Running with resource lock(s): "+this);

                writeService.concurrentTaskCount.incrementAndGet();
                
                // invoke doTask() on AbstractTask with locks.
                final Object ret = delegate.doTask();

                writeService.concurrentTaskCount.decrementAndGet();

                if(INFO)
                log.info("Did run with resource lock(s): "+this);
                
                lock.lock();
                
                try {
                
                    // success - checkpoint indices
                    checkpointIndices();
                    
                } finally {
                    
                    lock.unlock();
                    
                }

                return ret;
                
            } catch(Throwable t) {
                
                lock.lock();
                
                try {
                
                    // failure - discard partial writes.
                    rollbackIndices();
                    
                } finally {
                    
                    lock.unlock();
                    
                }
                
                throw new RuntimeException(t);
                
            } finally {
                
                /*
                 * Note: This is the ONLY place where it would be safe to turn
                 * autoCommit back on for the indices in the [indexCache]. As
                 * soon as we leave this method we have lost the exclusive lock
                 * on the index and another task could be running on it before
                 * we can do anything else.
                 */
                
                delegate.nanoTime_finishedWork = System.nanoTime();

            }
            
        }
        
        /**
         * Checkpoint each index accessed by the UNISOLATED task (this is necessary
         * in order to have the writes become restart safe since autoCommit is
         * disabled for UNISOLATED tasks). The task will participate in the next
         * commit. Since the index(s) for the task have already been checkpointed,
         * the commit will just record the address of that checkpoint record.
         * <p>
         * Note: This has the effect of flushing dirty indices to the store
         * immediately when the task completes. However, the nodes and leaves are
         * still in the cache on the index and will remain available if the index is
         * reused.
         */
        private void checkpointIndices() {

            if(INFO) log.info("Task accessed "+delegate.indexCache.size()+" indices: "+this);
            
            final Iterator<Map.Entry<String,IIndex>> itr = delegate.indexCache.entrySet().iterator();

            while (itr.hasNext()) {

                final Map.Entry<String, IIndex> entry = itr.next();
                
                final String name = entry.getKey();
                
                final IIndex tmp = entry.getValue();

                final BTree btree;
                
                if (tmp instanceof BTree) {

                    btree = ((BTree) tmp);

                } else {

                    btree = ((BTree) ((FusedView) tmp).getSources()[0]);

                }

                final boolean needsCheckpoint = btree.needsCheckpoint();
                
                if(INFO) log.info("name="+name+", needsCheckpoint="+needsCheckpoint+" : "+this);
                
                if(needsCheckpoint) {
                    
                    /*
                     * There are writes on the btree, so write a checkpoint
                     * record now that the task has completed successfully.
                     */
                    
                    final long checkpointAddr = btree.writeCheckpoint();
                    
                    if(INFO) log.info("name=" + name + ", newCheckpointAddr="
                            + btree.getStore().toString(checkpointAddr) + " : "
                            + this);
                    
                }

            }
            
        }
        
        /**
         * If the index(s) is dirty then we close it. The next time the index is
         * requested this will force the reload of the index from its last
         * checkpoint address as recorded in the last commit record. While the index
         * may have written data on the journal we just ignore it since it is
         * inaccessible and will disappear when the journal
         */
        private void rollbackIndices() {

            if(INFO) log.info("Rolling back "+delegate.indexCache.size()+" indices: "+this);
            
            final Iterator<Map.Entry<String,IIndex>> itr = delegate.indexCache.entrySet().iterator();

            while (itr.hasNext()) {

                final Map.Entry<String, IIndex> entry = itr.next();
                
                final String name = entry.getKey();
                
                final IIndex tmp = entry.getValue();

                final BTree btree;
                
                if (tmp instanceof BTree) {

                    btree = ((BTree) tmp);

                } else {

                    btree = ((BTree) ((FusedView) tmp).getSources()[0]);

                }
                
                final boolean needsCheckpoint = btree.needsCheckpoint();
                
                if(DEBUG) log.debug("name="+name+", needsCheckpoint="+needsCheckpoint+" : "+this);
                
                if(needsCheckpoint) {
                    
                    /*
                     * FIXME NOW : rollback semantics when task checkpoints indices.
                     * 
                     * This is not the precisely correct rollback condition. The
                     * issue is if the task itself checkpoints the index then
                     * the rollback point will be moved forward to that
                     * checkpoint since the btree will re-open from its last
                     * written checkpoint record. The solution is to keep a
                     * transient map from index name to the address of the
                     * rollback checkpoint. Changes to that map should be made
                     * atomically for all indices that were accessed by the
                     * UNISOLATED task using a scan of the [indexCache]. This
                     * can be made atomic using a lock governing access to the
                     * checkpoint rollback addresses.
                     */
                    
                    if(INFO) log.info("Rolling back index: "+name+" : "+this);
                    
                    if(btree.isOpen()) {
                        try {
                            btree.close();
                        } catch(Exception ex) {
                            log.warn("While rolling back index: "+name+" : "+this+" : "+ex);
                        }
                    }
                    
                }

            }
            
        }

    }
    
    /**
     * Return a view of the named index appropriate for the timestamp associated
     * with this task.
     * 
     * @param name
     *            The name of the index.
     * 
     * @throws NullPointerException
     *             if name is null.
     * @throws StaleLocatorException
     *             if the named index partition has been split, joined, or
     *             moved.
     * @throws NoSuchIndexException
     *             if the named index is not registered as of the timestamp.
     */
    final public IIndex getIndex(String name) {

        if (name == null) {

            throw new NullPointerException();
            
        }
        
        if(aborted) {
            
            /*
             * The task has been interrupted by the write service which also
             * sets the [aborted] flag since the interrupt status may have been
             * cleared by looking at it.
             */

            throw new RuntimeException(new InterruptedException());
            
        }
        
        /*
         * Test the named index cache first.
         */
        {

            final IIndex index = indexCache.get(name);

            if (index != null) {

                // Cached value.
                return index;

            }

        }

        // validate that this is a declared index.
        assertResource(name);
        
        final IIndex tmp = resourceManager.getIndex(name, timestamp);
        
        if (tmp == null) {

            // Presume client has made a bad request.
            throw new NoSuchIndexException(name + ", timestamp=" + timestamp);

        }
        
        /*
         * We turn off auto-commit on the index for UNISOLATED writers. This is
         * critical to prevent partial writes from being made restart safe when
         * a task fails.
         */
        
        if (timestamp == ITx.UNISOLATED) {

            log.debug("Disabling autoCommit: "+name+" : "+this);
            
            if (tmp instanceof BTree) {

                ((BTree) tmp).setAutoCommit(false);

            } else {

                ((BTree) ((FusedView) tmp).getSources()[0])
                        .setAutoCommit(false);

            }

        }
        
        /*
         * Put the index into a hard reference cache under its name so that we
         * can hold onto it for the duration of the operation.
         */

        indexCache.put(name, tmp);

        return tmp;

    }

    /**
     * Release hard references to named indices. Dirty indices will exist on the
     * Name2Addr's commitList until the next commit.
     */
    private void clearIndexCache() {

        log.info("Clearing hard reference cache: "+indexCache.size()+" indices accessed");
        
        if (timestamp == ITx.UNISOLATED || timestamp == ITx.READ_COMMITTED) {
            
            /*
             * Report counters for unisolated and read-committed indices.
             */
            
            Iterator<Map.Entry<String, IIndex>> itr = indexCache.entrySet()
                    .iterator();

            while (itr.hasNext()) {

                final Map.Entry<String, IIndex> entry = itr.next();

                final String name = entry.getKey();

                final IIndex ndx = entry.getValue();

                ((ConcurrencyManager) concurrencyManager)
                        .addCounters(name, ndx);
                
            }
        
        }
        
        indexCache.clear();

    }
    
    /**
     * This is thrown if you attempt to reuse (re-submit) the same
     * {@link AbstractTask} instance.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ResubmitException extends RejectedExecutionException {

        /**
         * 
         */
        private static final long serialVersionUID = -8661545948587322943L;
        
        public ResubmitException() {
            super();
        }
        
        public ResubmitException(String msg) {
            
            super(msg);
            
        }

    }
    
    /**
     * A read-write view of an {@link IJournal} that is used to impose isolated
     * and atomic changes for {@link ITx#UNISOLATED} tasks that register or drop
     * indices. The intentions of the task are buffered locally (by this class)
     * for that task such that the index appears to have been registered or
     * dropped immediately. Those intentions are propagated to {@link Name2Addr}
     * on a task-by-task basis only when (a) this task is part of the current
     * commit group; and (b) the {@link WriteExecutorService} performs a group
     * commit. If there is a conflict between the state of {@link Name2Addr} at
     * the time of the commit and the intentions of the tasks in the commit
     * group (for example, if two tasks try to drop the same index), then the
     * 2nd task will be interrupted. The enumeration of the intentions of tasks
     * always begins with the task executed by the thread performing the commit
     * so that no other task's intention could cause the commit itself to be
     * interrupted.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class AtomicRegisterDropJournal implements IJournal {
        
        private final AbstractJournal delegate;
        
        abstract class Action {

            final String name;
            private Action priorAction = null;

            private Action() {
                throw new UnsupportedOperationException();
            }
            
            Action(String name) {
            
                if (name == null)
                    throw new IllegalArgumentException();
                
                this.name = name;
                
            }
            
            abstract boolean isRegister();
            
            abstract boolean isDrop();
            
            final protected Action getPriorAction() {
                
                return priorAction;
                
            }
            
            abstract void doAction();
            
            /**
             * Chains together actions on the same named index so that we can
             * apply the actions in the same sequence in {@link #doAction()}
             * during the commit.
             * 
             * @param priorAction
             *            The previous action on the named index.
             */
            void setPreviousAction(Action priorAction) {

                if (priorAction == null)
                    throw new IllegalArgumentException();
                
                if (this.priorAction != null) {

                    throw new IllegalStateException();
                    
                }
                
                this.priorAction = priorAction;
                
            }
            
        }
        
        /**
         * Action to drop an index.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        class DropAction extends Action {

            DropAction(String name) {
                super(name);
            }
            
            final boolean isRegister() {return false;}
            
            final boolean isDrop() {return true;}

            /**
             * Drops the index on the <strong>delegate</strong>.
             */
            final void doAction() {

                final Action priorAction = getPriorAction();
                
                if (priorAction != null) {

                    // recursively handle previous actions first.
                    priorAction.doAction();
                    
                }
                
                delegate.dropIndex(name);
                
            }
            
        }
        
        /**
         * Action to register an index.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        class RegisterAction extends Action {
            
            final BTree btree;

            RegisterAction(String name,BTree btree) {
                
                super(name);
                
                if (btree == null)
                    throw new IllegalArgumentException();
                
                this.btree = btree;
                
            }
            
            final boolean isRegister() {return true;}
            
            final boolean isDrop() {return false;}
            
            /**
             * Registers the index on the <strong>delegate</strong>.
             */
            final void doAction() {

                final Action priorAction = getPriorAction();
                
                if (priorAction != null) {

                    // recursively handle previous actions first.
                    priorAction.doAction();
                    
                }
                
                delegate.registerIndex(name, btree);
                
            }

        }
        
        /**
         * Local cache of actions is interposed between the {@link AbstractTask}
         * and the {@link Name2Addr} instance used by the {@link #delegate}
         * journal.
         */
        private final Map<String,Action> actions = new HashMap<String, Action>();

        /**
         * FIXME NOW : Use this instead of directly exposing the
         * {@link AbstractJournal} to the task. Once this change is applied and
         * until I get this right I may be unable to add/drop indices!
         * 
         * @param source
         */
        public AtomicRegisterDropJournal(AbstractJournal source) {

            if (source == null)
                throw new IllegalArgumentException();

            this.delegate = source;

        }

        /*
         * Overriden methods for registering or dropping indices.
         */
        
        synchronized public void dropIndex(String name) {
            
            final Action lastAction = actions.get(name);
            
            if (lastAction != null) {
                
                if (lastAction.isDrop()) {
                    
                    /*
                     * The index would not exist since it was dropped by a prior
                     * action.
                     */
                    
                    throw new NoSuchIndexException(name);
                    
                } else {
                    
                    /*
                     * Drop an index that was already registered by this task?
                     * 
                     * Note: We replace the action in the local cache, but we
                     * also chain together the actions so that they are both
                     * applied during the commit protocol.
                     */
                    
                    final DropAction newAction = new DropAction(name);
                    
                    newAction.setPreviousAction(lastAction);

                    actions.put(name, newAction);

                }

            } else {

                /*
                 * Drop an index (1st action for that index).
                 */

                if (delegate.getIndex(name) == null) {

                    // Index was not defined when this task started.

                    throw new NoSuchIndexException(name);

                }

                // index will be dropped iff this task completes.
                actions.put(name, new DropAction(name));

            }
            
        }

        /**
         * Note: This is the core implementation for registering an index - it
         * handles the local cache.
         */
        synchronized public BTree registerIndex(String name, BTree btree) {

            final Action lastAction = actions.get(name);
            
            if (lastAction != null) {
                
                if (lastAction.isRegister()) {
                    
                    // can not double-register an index.
                    
                    throw new IndexExistsException(name);
                    
                } else {
                    
                    /*
                     * Register an index which was already dropped by this
                     * task.
                     * 
                     * Note: We replace the action in the local cache, but we
                     * also chain together the actions so that they are both
                     * applied during the commit protocol. This is necessary in
                     * order for a sequence which drops a pre-existing index and
                     * then registers a new index under the same name to work!
                     */
                    
                    actions.remove(name);
                    
                    final RegisterAction newAction = new RegisterAction(name,btree);
                    
                    newAction.setPreviousAction(lastAction);
                    
                    actions.put(name,newAction);
                    
                }
                
            } else {

                /*
                 * Register an index (first action for that index). 
                 */
                
                if(delegate.getIndex(name) != null) {
                
                    /*
                     * The index already exists.
                     */
                    
                    throw new IndexExistsException(name);
                    
                } else {

                    /*
                     * The task will register the index iff it commits.
                     */
                    
                    actions.put(name, new RegisterAction(name, btree));
                    
                }
                
            }
            
            return btree;
            
        }

        public void registerIndex(IndexMetadata indexMetadata) {
         
            // delegate to core impl.
            registerIndex(indexMetadata.getName(), indexMetadata);
            
        }

        public BTree registerIndex(String name, IndexMetadata indexMetadata) {
            
            // Note: handles constraints and defaults for index partitions.
            delegate.validateIndexMetadata(name, indexMetadata);
            
            // Note: create on the _delegate_.
            final BTree btree = BTree.create(delegate, indexMetadata);

            // delegate to core impl.
            return registerIndex(name, btree);
            
        }

        /**
         * Propagates add/drop actions to {@link Name2Addr} on the backing
         * journal.
         * <p>
         * Note: it is sufficient to simply execute the actions in sequence
         * against the backing group once we have reached the commit point for
         * all tasks in the commit group. The backing journal will delegate
         * those actions to its {@link Name2Addr} object, which will accept or
         * reject the actions based on its then current state.
         * 
         * FIXME NOW : actions which are rejected MUST cause the task to be
         * interrupted (or otherwise through out an exception, ideally one
         * wrapping the {@link NoSuchIndexException} or
         * {@link IndexExistsException})
         * 
         * FIXME NOW : Tasks which fail MUST have their checkpoints discarded
         * (this may require that we execute the tasks in the strict order in
         * which they wrote their checkpoints), and only the failed tasks (or
         * any tasks which were dependent on their success) should fail.
         * 
         * FIXME NOW : Make sure that this gets invoked and watch the order in
         * which we invoke this method for each task in a commit group. We can
         * FLUSH the root of the index at any time. If we checkpoint the index
         * in the task, then rollback needs to re-write the original (pre-task)
         * checkpoint record.
         * 
         * FIXME NOW : should tasks within the same commit group be allowed to
         * read or write on the same index, thereby creating a dependency on the
         * success of other tasks in that commit group? if we allow this then we
         * need to cause those task(s) to fail if the tasks which have become
         * essentially their pre-conditions fail during the commit. If we will
         * disallow this, then a mechanism must be put into place to prevent
         * this situation.
         */
        protected void commitHelper() throws NoSuchIndexException,
                IndexExistsException {

            for(Action action : actions.values()) {

                action.doAction();
                
            }

        }

        /**
         * Tests the local {@link Action} cache and delegates to the backing journal
         * iff there was no cached {@link Action}.
         */
        public IIndex getIndex(String name) {

            final Action lastAction = actions.get(name);
            
            if (lastAction != null) {

                if (lastAction.isDrop()) {

                    // index was dropped.
                    return null;

                } else {

                    // index was registered.
                    return ((RegisterAction) lastAction).btree;
                
                }

            } else {

                // no local action, send request to the delegate.
                return delegate.getIndex(name);

            }
            
        }

        public IIndex getIndex(String name, long timestamp) {
            
            if (timestamp == ITx.UNISOLATED) {

                // unisolated index requests must test the local cache first.
                return getIndex(name);
                
            }
            
            return delegate.getIndex(name, timestamp);
            
        }

        /*
         * Disallowed methods.
         */
        
        public void abort() {
            throw new UnsupportedOperationException();
        }

        public void close() {
            throw new UnsupportedOperationException();
        }

        public void closeAndDelete() {
            throw new UnsupportedOperationException();
        }

        public long commit() {
            throw new UnsupportedOperationException();
        }

        public void deleteResources() {
            delegate.deleteResources();
        }

        public Object deserialize(byte[] b, int off, int len) {
            return delegate.deserialize(b, off, len);
        }

        public Object deserialize(byte[] b) {
            return delegate.deserialize(b);
        }

        public Object deserialize(ByteBuffer buf) {
            return delegate.deserialize(buf);
        }

        public void discardCommitters() {
            throw new UnsupportedOperationException();
        }

        public void force(boolean metadata) {
            delegate.force(metadata);
        }

        public int getByteCount(long addr) {
            return delegate.getByteCount(addr);
        }

        public ICommitRecord getCommitRecord(long timestamp) {
            return delegate.getCommitRecord(timestamp);
        }

        public CounterSet getCounters() {
            return delegate.getCounters();
        }

        public File getFile() {
            return delegate.getFile();
        }

        public long getOffset(long addr) {
            return delegate.getOffset(addr);
        }

        public Properties getProperties() {
            return delegate.getProperties();
        }

        public IResourceMetadata getResourceMetadata() {
            return delegate.getResourceMetadata();
        }

        public long getRootAddr(int index) {
            return delegate.getRootAddr(index);
        }

        public IRootBlockView getRootBlockView() {
            return delegate.getRootBlockView();
        }

        public boolean isFullyBuffered() {
            return delegate.isFullyBuffered();
        }

        public boolean isOpen() {
            return delegate.isOpen();
        }

        public boolean isReadOnly() {
            return delegate.isReadOnly();
        }

        public boolean isStable() {
            return delegate.isStable();
        }

        public void packAddr(DataOutput out, long addr) throws IOException {
            delegate.packAddr(out, addr);
        }

        public ByteBuffer read(long addr) {
            return delegate.read(addr);
        }

        public byte[] serialize(Object obj) {
            return delegate.serialize(obj);
        }

        public void setCommitter(int index, ICommitter committer) {
            delegate.setCommitter(index, committer);
        }

        public void setupCommitters() {
            delegate.setupCommitters();
        }

        public void shutdown() {
            delegate.shutdown();
        }

        public void shutdownNow() {
            delegate.shutdownNow();
        }

        public long size() {
            return delegate.size();
        }

        public long toAddr(int nbytes, long offset) {
            return delegate.toAddr(nbytes, offset);
        }

        public String toString(long addr) {
            return delegate.toString(addr);
        }

        public long unpackAddr(DataInput in) throws IOException {
            return delegate.unpackAddr(in);
        }

        public long write(ByteBuffer data) {
            return delegate.write(data);
        }

    }
    
    /**
     * A read-only view of an {@link IJournal} that is used to enforce read-only
     * semantics on tasks using {@link AbstractTask#getJournal()} to access the
     * backing store.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class ReadOnlyJournal implements IJournal {

        private final IJournal delegate;
        
        public ReadOnlyJournal(IJournal source) {

            if (source == null)
                throw new IllegalArgumentException();

            this.delegate = source;

        }

        public void abort() {
            throw new UnsupportedOperationException();
        }

        public void close() {
            throw new UnsupportedOperationException();
        }

        public void closeAndDelete() {
            throw new UnsupportedOperationException();
        }

        public long commit() {
            throw new UnsupportedOperationException();
        }

        public void deleteResources() {
            throw new UnsupportedOperationException();
        }

        public Object deserialize(byte[] b, int off, int len) {
            return delegate.deserialize(b, off, len);
        }

        public Object deserialize(byte[] b) {
            return delegate.deserialize(b);
        }

        public Object deserialize(ByteBuffer buf) {
            return delegate.deserialize(buf);
        }

        public void discardCommitters() {
            throw new UnsupportedOperationException();
        }

        public void dropIndex(String name) {
            throw new UnsupportedOperationException();
        }

        public void force(boolean metadata) {
            throw new UnsupportedOperationException();
        }

        public int getByteCount(long addr) {
            return delegate.getByteCount(addr);
        }

        public ICommitRecord getCommitRecord(long timestamp) {
            return delegate.getCommitRecord(timestamp);
        }

        public CounterSet getCounters() {
            return delegate.getCounters();
        }

        public File getFile() {
            return delegate.getFile();
        }

        /**
         * Note: Does not allow access to {@link ITx#UNISOLATED} indices.
         */
        public IIndex getIndex(String name, long timestamp) {

            if (timestamp == ITx.UNISOLATED)
                throw new UnsupportedOperationException();

            return delegate.getIndex(name, timestamp);

        }

        /**
         * Note: Not supported since this method returns the {@link ITx#UNISOLATED}
         * index.
         */
        public IIndex getIndex(String name) {
            throw new UnsupportedOperationException();
        }

        public long getOffset(long addr) {
            return delegate.getOffset(addr);
        }

        public Properties getProperties() {
            return delegate.getProperties();
        }

        public IResourceMetadata getResourceMetadata() {
            return delegate.getResourceMetadata();
        }

        public long getRootAddr(int index) {
            return delegate.getRootAddr(index);
        }

        public IRootBlockView getRootBlockView() {
            return delegate.getRootBlockView();
        }

        public boolean isFullyBuffered() {
            return delegate.isFullyBuffered();
        }

        public boolean isOpen() {
            return delegate.isOpen();
        }

        public boolean isReadOnly() {
            return delegate.isReadOnly();
        }

        public boolean isStable() {
            return delegate.isStable();
        }

        public void packAddr(DataOutput out, long addr) throws IOException {
            delegate.packAddr(out, addr);
        }

        public ByteBuffer read(long addr) {
            return delegate.read(addr);
        }

        public void registerIndex(IndexMetadata indexMetadata) {
            throw new UnsupportedOperationException();
        }

        public IIndex registerIndex(String name, BTree btree) {
            throw new UnsupportedOperationException();
        }

        public IIndex registerIndex(String name, IndexMetadata indexMetadata) {
            throw new UnsupportedOperationException();
        }

        public byte[] serialize(Object obj) {
            return delegate.serialize(obj);
        }

        public void setCommitter(int index, ICommitter committer) {
            delegate.setCommitter(index, committer);
        }

        public void setupCommitters() {
            delegate.setupCommitters();
        }

        public void shutdown() {
            delegate.shutdown();
        }

        public void shutdownNow() {
            delegate.shutdownNow();
        }

        public long size() {
            return delegate.size();
        }

        public long toAddr(int nbytes, long offset) {
            return delegate.toAddr(nbytes, offset);
        }

        public String toString(long addr) {
            return delegate.toString(addr);
        }

        public long unpackAddr(DataInput in) throws IOException {
            return delegate.unpackAddr(in);
        }

        public long write(ByteBuffer data) {
            throw new UnsupportedOperationException();
        }

    }

}
