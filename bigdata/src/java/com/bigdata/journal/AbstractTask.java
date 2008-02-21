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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.concurrent.LockManager;
import com.bigdata.concurrent.LockManagerTask;

/**
 * Abstract base class for tasks that may be submitted to an
 * {@link IConcurrencyManager}. Tasks may be isolated (by a transaction) or
 * unisolated. Tasks access named resources (aka indices), which they
 * pre-declare in their constructors.
 * <p>
 * Isolated tasks are part of a larger transaction. Transactions are started and
 * committed using an {@link ITransactionManagerService}. Several kinds of
 * isolation are supported - see {@link IsolationEnum}. Transactional tasks run
 * with full concurrency using an MVCC (Multi-Version Concurrency Control)
 * strategy. When a transaction is committed (by the
 * {@link ITransactionManagerService}) it must wait for lock(s) on the
 * unisolated named indices on which it has written before it may validate and
 * commit.
 * <p>
 * Unisolated tasks are further divided into read only and read/write. A
 * read-only task reads against the last committed state of a named index.
 * Concurrent unisolated readers are permitted without locking on the same
 * index.
 * <p>
 * A read/write task reads and writes on the "live" index. Note that only a
 * single thread may write on a {@link BTree} at a time. Therefore read/write
 * tasks (often referred to as writers) obtain an exclusive lock on the named
 * index(s). When more than one named index is used, the locks are used to infer
 * a partial ordering of the writers allowing as much concurrency as possible.
 * Pre-declaration of locks allows us to avoid deadlocks in the lock system.
 * <p>
 * Note: Use a distinct instance of this task each time you
 * {@link ConcurrencyManager#submit(AbstractTask)} it!
 * 
 * @todo review javadoc here and on {@link AbstractJournal} and the
 *       {@link IConcurrencyManager} interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractTask implements Callable<Object> {

    static protected final Logger log = Logger.getLogger(AbstractTask.class);

    /**
     * Used to protect against re-submission of the same task object.
     */
    private final AtomicBoolean submitted = new AtomicBoolean(false);

    /**
     * The object used to manage exclusive access to the unisolated indices.
     */
    protected final IConcurrencyManager concurrencyManager;
    
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
     * The journal against which the operation will be carried out.
     * <p>
     * If the task is running against an unisolated index, then this will be the
     * {@link IResourceManager#getLiveJournal()}. Otherwise it will be whatever
     * journal is appropriate to the historical commit point against which the
     * task is being run.
     * <p>
     * Note: This exposes unconstrained access to the journal that could be used
     * to violate the concurrency control mechanisms, therefore you SHOULD NOT
     * use this unless you have a clear idea what you are about. You should be
     * able to write all application level tasks in terms of
     * {@link #getIndex(String)} and operations on the returned index. Using
     * {@link SequenceTask} you can combine application specific unisolated
     * write tasks with tasks that add or drop indices into atomic operations.
     * 
     * @return The corresponding journal for that timestamp -or-
     *         <code>null</code> if no journal has data for that timestamp,
     *         including when a historical journal with data for that timestamp
     *         has been deleted.
     *         
     * @see IResourceManager#getJournal(long)
     */
    protected final AbstractJournal getJournal() {
        
        final AbstractJournal journal = resourceManager.getJournal(Math
                .abs(startTime));
        
        log.warn("No such journal: timestamp=" + startTime);
        
        return journal;
        
    }
    
//    /**
//     * Find and cache the appropriate commit record for this task.
//     * 
//     * @return The commit record -or- <code>null</code> iff there is no commit
//     *         record corresponding to the {@link #startTime} for this task.
//     */
//    protected final ICommitRecord getCommitRecord() {
//        
//        if (commitRecord == null) {
//            
//            final AbstractJournal journal = getJournal();
//
//            if (journal == null) return null;
//            
//            if (startTime < ITx.UNISOLATED) {
//
//                /*
//                 * Historical read task.
//                 */
//                
//                commitRecord = journal.getCommitRecord(-startTime);
//
//                if (commitRecord == null) {
//
//                    log.warn("No commit point less than startTime="
//                            + (-startTime));
//
//                    return null;
//                    
//                }
//
//                log.info("Found historical committed state"+
//                        ": startTime=" + startTime +
//                        ", commitCounter=" + commitRecord.getCommitCounter() +
//                        ", timestamp="+ commitRecord.getTimestamp()
//                        );
//
//            } else {
//
//                /*
//                 * Read only task. We note the most recent commit record so that
//                 * we can base all read-only views off of the same consistent
//                 * historical state (and so that we do not conflict with
//                 * unisolated writers on the live versions of the indices).
//                 */
//
//                commitRecord = journal.getCommitRecord();
//
//                log.info("Found most recent committed state"+
//                        ": commitCounter="+ commitRecord.getCommitCounter()+
//                        ", timestamp="+ commitRecord.getTimestamp()
//                        );
//
//            }
//
//        }
//
//        return commitRecord;
//        
//    }

    /**
     * The transaction identifier -or- {@link ITx#UNISOLATED} IFF the operation
     * is NOT isolated by a transaction -or- <code> - startTime </code> to read
     * from the most recent commit point not later than the absolute value of
     * <i>startTime</i>.
     */
    protected final long startTime;

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

//    /**
//     * We note and save a reference to the most recent commit record when we
//     * begin a task that is an <strong>unisolated reader</strong>. This allows
//     * us to ensure that all index views requested by that reader are (a) from
//     * the same committed state of the store; and (b) do NOT conflict with
//     * unisolated writers (which are NOT thread-safe).
//     */
//    private ICommitRecord commitRecord = null;
    
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
     * Convenience constructor variant for one named resource.
     * 
     * @param concurrencyControl
     *            The object used to control access to the local resources.
     * @param startTime
     *            The transaction identifier -or- {@link ITx#UNISOLATED} IFF the
     *            operation is NOT isolated by a transaction -or-
     *            <code> - tx </code> to read from the most recent commit point
     *            not later than the absolute value of <i>tx</i> (a fully
     *            isolated read-only transaction using a historical start time).
     * @param resource
     *            The resource on which the task will operate. E.g., the names
     *            of the index. When the task is an unisolated write task an
     *            exclusive lock will be requested on the named resource and the
     *            task will NOT run until it has obtained that lock.
     */
    protected AbstractTask(IConcurrencyManager concurrencyManager,
            long startTime, String resource) {

        this(concurrencyManager, startTime, new String[] { resource });
        
    }
    
    /**
     * 
     * @param concurrencyControl
     *            The object used to control access to the local resources.
     * @param startTime
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
            long startTime, String[] resource) {
        
        if (concurrencyManager == null) {

            throw new NullPointerException();

        }

//        if (startTime < ITx.UNISOLATED && !readOnly) {
//
//            throw new IllegalArgumentException(
//                    "Historical reads must be flagged as read-only");
//
//        }
//
//        if (startTime == ITx.UNISOLATED && readOnly) {
//
//            /*
//             * Note: For this you generally want to use ITx.READ_COMMITTED
//             * instead.
//             */
//            
//            throw new IllegalArgumentException(
//                    "Unisolated operations may not be flagged as read-only.");
//            
//        }
        
        if (resource == null) {

            throw new NullPointerException();
            
        }

        for(int i=0; i<resource.length; i++) {
            
            if (resource[i] == null) {

                throw new NullPointerException();
                
            }
            
        }
        
        this.concurrencyManager = concurrencyManager;

        this.transactionManager = concurrencyManager.getTransactionManager();

        this.resourceManager = concurrencyManager.getResourceManager();
        
        this.startTime = startTime;

        this.isTransaction = startTime > ITx.UNISOLATED;

//        this.readOnly = readOnly;
        
        this.resource = resource;

        this.indexCache = new HashMap<String,IIndex>(resource.length);
        
        if (startTime > ITx.UNISOLATED) {

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

            tx = transactionManager.getTx(startTime);

            if (tx == null) {

                throw new IllegalStateException("Unknown tx");

            }

            if (!tx.isActive()) {

                throw new IllegalStateException("Tx not active");

            }

            readOnly = tx.isReadOnly();
            
//            if (tx.isReadOnly() != readOnly) {
//
//                throw new IllegalArgumentException("Tx readOnly="
//                        + tx.isReadOnly() + ", but task readOnly="
//                        + readOnly);
//                
//            }

        } else if (startTime < ITx.UNISOLATED) {

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
            
        } else {

            /*
             * Unisolated operation.
             */

            tx = null;
            
            readOnly = false;

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
     * {@link ExecutionException}s by the {@link ExecutorService}.
     * 
     * @throws {@link ResubmitException}
     *             if the task has already been submitted.
     * @throws {@link RetryException}
     *             if the task MAY be retried.
     * @throws {@link InterruptedException}
     *             can be thrown if the task is interrupted, for example while
     *             awaiting a lock, if the commit group is being discarded, or
     *             if the journal is being shutdown (which will cause the
     *             executor service running the task to be shutdown and thereby
     *             interrupt all running tasks).
     * 
     * @todo document other exceptions that can be thrown here.
     */
    final public Object call() throws Exception {

        if (!submitted.compareAndSet(false, true)) {

            throw new ResubmitException(getClass().getName());
            
        }
        
        try {
            
            if (isTransaction) {

                log.info("Running isolated operation: tx="+startTime);
                
                if(tx.isReadOnly()) {

                    try {

                        return doTask();

                    } finally {
                        
                        // release hard references to named read-only indices.
                        
                        indexCache.clear();
                        
                    }

                }
                
                /*
                 * Delegate handles handshaking for writable transactions.
                 */

                Callable<Object> delegate = new InnerReadWriteTxServiceCallable(tx);
                
                return delegate.call();

            }

            if (readOnly) {

                try {

                    return doTask();
                    
                } finally {
                    
                    // release hard references to the named read-only indices.
                    
                    indexCache.clear();
                
                    log.info("Reader is done: startTime="+startTime);
                    
                }

            } else {

                /*
                 * Handle unisolated write tasks, which need to coordinate with
                 * the lock manager for the unisolated indices.
                 */

                assert startTime == ITx.UNISOLATED;
                
                return doUnisolatedReadWriteTask();
                
            }

        } finally {

            log.info("done");

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

        log.info("Unisolated write task: resources="
                + Arrays.toString(resource));

        // declare resource(s).
        lockManager.addResource(resource);

        // delegate will handle lock acquisition and invoke doTask().
        Callable<Object> delegate = new LockManagerTask<String>(
                lockManager, resource, new InnerWriteServiceCallable());
        
        final WriteExecutorService writeService = concurrencyManager.getWriteService();

        writeService.beforeTask(Thread.currentThread(), this);

        boolean ran = false;

        try {

            final Object ret;
            
            try {

                /*
                 * Note: The lock(s) are only held during this call. By the time
                 * the call returns any lock(s) have been released. Locks MUST
                 * be released as soon as the task is done writing so that it
                 * does NOT hold locks while it is awaiting commit. This make it
                 * possible for other operations to write on the same index in
                 * the same commit group.
                 */

                ret = delegate.call();

                if(Thread.interrupted()) {
                    
                    throw new InterruptedException();
                    
                }

                // set flag.
                ran = true;
                
                log.info("Task Ok: class="+getClass().getName());
                
                /*
                 * Note: I am choosing NOT to flush dirty indices to the store
                 * after each task in case other tasks in the same commit group
                 * want to write on the same index. Flushing an index makes its
                 * nodes and leaves immutable, and that is not desirable if you
                 * are going to write on it again soon.
                 * 
                 * Note: This trades off against checkpointing the indices after
                 * each task which might make it possible to discard only part
                 * of a commit group.
                 */

            } finally {

                /*
                 * Release hard references to named indices. Dirty indices will
                 * exist on the Name2Addr's commitList until the next commit.
                 */

                indexCache.clear();

            }
            
            /*
             * Note: The WriteServiceExecutor will await a commit signal
             * before the thread is allowed to complete. This ensures
             * that the caller waits until a commit (or an abort).
             * 
             * Note: Waiting here does NOT prevent other tasks from
             * gaining access to the same resources since the locks were
             * released above.
             */ 

            writeService.afterTask(this, null);

            return ret;

        } catch (Throwable t) {

            if (!ran) {

                // Do not re-invoke it afterTask failed above.

                log.info("Task failed: class="+getClass().getName()+" : "+t);
                
                writeService.afterTask(this, t);

            }

            /*
             * Throw whatever exception was thrown by the task (or by afterTask
             * if it craps out).
             */
            
            if (t instanceof Exception)
                throw (Exception) t;

            throw new RuntimeException(t);

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
     * FIXME Modify the {@link WriteExecutorService} to use checkpoints on named
     * indices after each task. This will not only allow us to abort individual
     * tasks that fail (without discarding the commit group), it will also allow
     * us to commit without waiting for long running tasks to complete (since
     * they are executing they have an exclusive lock on the named indices so
     * noone else can write while they are running). The same solution should be
     * applied to the within transaction contention for isolated indices writing
     * on the temporary store for the transaction.
     * <p>
     * An index checkpoint simply flushes the index (and its metadata record) to
     * disk. This approach REQUIRES that we do NOT mark the index as requiring
     * commit until the task has completed successfully, so there MUST be an
     * explicit signal from the
     * {@link WriteExecutorService#afterTask(AbstractTask, Throwable)} to
     * {@link Name2Addr} signaling that the index is (a) dirty, and (b) should
     * be put onto the commitList. At that point the task waits and it will
     * participate in the next commit group. Once the index gets onto the
     * {@link Name2Addr} commitList and is committed, it will automatically be
     * available for subsequent tasks. If a task fails, then we just re-load the
     * last committed state of the index using {@link Name2Addr} - this is the
     * standard behavior in any case.
     */
    class InnerReadWriteTxServiceCallable implements Callable<Object> {

        final ITx tx;
        
        InnerReadWriteTxServiceCallable(ITx tx) {
            
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

                return doTask();
                
            } finally {
                
                /*
                 * Release hard references to the named indices. The backing
                 * indices are reading from the ground state identified by the
                 * start time of the ReadWrite transaction.
                 */
                
                indexCache.clear();
                
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
    class InnerWriteServiceCallable implements Callable<Object> {

        InnerWriteServiceCallable() {
        }

        /**
         * 
         */
        public Object call() throws Exception {

            return doTask();
            
        }

    }
    
    /**
     * Return an appropriate view of the named index for the operation.
     * <p>
     * When the task is isolated by a transaction, then the index will be
     * isolated by the transaction using the appropriate
     * {@link IsolationEnum isolation level}. If the transaction is read-only,
     * then the index will not be writable.
     * </p>
     * <p>
     * When the task is a read-only unisolated operation, the index will be
     * read-only and will read from the most recent committed state of the store
     * prior to the time at which the task began to execute. If multiple index
     * views are requested they will all use the same committed state of the
     * store.
     * </p>
     * <p>
     * When the task is an unisolated write operation the index will be the
     * unisolated writable (aka "live" or "current" index). Access to the
     * unisolated writable indices is single-threaded. This constraint is
     * enforced by a lock system using the named resources declared in the task
     * constructor.
     * </p>
     * 
     * @param name
     *            The index name.
     * 
     * @return An appropriate view of the named index.
     * 
     * @exception NoSuchIndexException
     *                if the named index does not exist at the time that the
     *                operation is executed.
     * 
     * @exception IllegalStateException
     *                if the named index is not one of the resources declared to
     *                the constructor.
     * 
     * @todo Make sure that we have tests for historical reads (including
     *       readOnly with timestamp == 0L), unisolated read-write tasks, and
     *       various kinds of transactions (timestamp > 0L). Also verify when
     *       the indices are views with more than one resource (e.g., there are
     *       one or more index segments that are part of the view).
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
        
        final IIndex tmp = transactionManager.getIndex(name, startTime);
        
        if (tmp == null) {

            throw new NoSuchIndexException(name + " @ " + startTime);

        }
        
// final IIndex tmp;
//
//        if (isTransaction) {
//
//            /*
//             * Isolated operation.
//             * 
//             * Note: The backing index is always a historical state of the named
//             * index.
//             */
//
//            final IIndex isolatedIndex = tx.getIndex(name);
//
//            if (isolatedIndex == null) {
//
//                throw new NoSuchIndexException(name);
//
//            }
//
//            tmp = isolatedIndex;
//
//        } else {
//            
//            /*
//             * historical read -or- unisolated read operation.
//             */
//
//            if (readOnly) {
//
//                final AbstractBTree[] sources = resourceManager
//                        .getIndexSources(name, startTime);
//
//                if (sources == null) {
//
//                    throw new NoSuchIndexException(name);
//
//                }
//
//                if (sources.length == 1) {
//
//                    tmp = new ReadOnlyIndex( sources[0] );
//
//                } else {
//
//                    tmp = new ReadOnlyFusedView(sources);
//
//                }
//                            
//            } else {
//                
//                /*
//                 * Writable unisolated index.
//                 * 
//                 * Note: This is the "live" mutable index. This index is NOT
//                 * thread-safe. A lock manager is used to ensure that at most
//                 * one task has access to this index at a time.
//                 */
//
//                assert startTime == ITx.UNISOLATED;
//                
//                final AbstractBTree[] sources = resourceManager
//                        .getIndexSources(name, ITx.UNISOLATED);
//                
//                if (sources == null) {
//
//                    throw new NoSuchIndexException(name);
//
//                }
//
//                if (sources.length == 1) {
//
//                    tmp = sources[0];
//                    
//                } else {
//                    
//                    tmp = new FusedView( sources );
//                    
//                }
//
//            }
//
//        }

        /*
         * Put the index into a hard reference cache under its name so that we
         * can hold onto it for the duration of the operation.
         */

        indexCache.put(name, tmp);

        return tmp;

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
    
}
