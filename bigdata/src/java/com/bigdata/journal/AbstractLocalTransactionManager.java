package com.bigdata.journal;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.util.InnerCause;

/**
 * Manages the client side of a transaction either for a standalone
 * {@link Journal} or for an {@link IDataService} in an
 * {@link IBigdataFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractLocalTransactionManager extends TimestampUtility
        implements ILocalTransactionManager {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(IJournal.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.isDebugEnabled();

    private IConcurrencyManager concurrencyManager;
    
    /**
     * The object used to control access to the index resources.
     * 
     * @throws IllegalStateException
     *             if the object has not been set yet using
     *             {@link #setConcurrencyManager(IConcurrencyManager)}.
     */
    public IConcurrencyManager getConcurrencyManager() {

        if (concurrencyManager == null) {

            // Not assigned!

            throw new IllegalStateException();

        }

        return concurrencyManager;

    }

    public void setConcurrencyManager(IConcurrencyManager concurrencyManager) {

        if (concurrencyManager == null)
            throw new IllegalArgumentException();

        if (this.concurrencyManager != null)
            throw new IllegalStateException();

        this.concurrencyManager = concurrencyManager;
        
    }

    /**
     * Note: You MUST use {@link #setConcurrencyManager(IConcurrencyManager)}
     * after calling this constructor (the parameter can not be passed in since
     * there is a circular dependency between the {@link IConcurrencyManager}
     * and {@link #commit(long)} on this class, which requires access to the
     * {@link IConcurrencyManager} to submit a task).
     */
    public AbstractLocalTransactionManager() {

    }

    /*
     * ILocalTransactionManager
     */
    
    /**
     * A hash map containing all active transactions. A transaction that is
     * preparing will remain in this collection until it has either successfully
     * prepared or aborted.
     */
    final Map<Long, ITx> activeTx = new ConcurrentHashMap<Long, ITx>();

    /**
     * A hash map containing all transactions that have prepared but not yet
     * either committed or aborted.
     * 
     * @todo A transaction will be in this map only while it is actively
     *       committing, so this is always a "map" of one and could be replaced
     *       by a scalar reference (except that we may allow concurrent prepare
     *       and commit of read-only transactions).
     */
    final Map<Long, ITx> preparedTx = new ConcurrentHashMap<Long, ITx>();

//    public int getActiveTxCount() {
//        
//        return activeTx.size();
//        
//    }
//
//    public int getPreparedTxCount() {
//        
//        return preparedTx.size();
//        
//    }
    
    /**
     * Notify the journal that a new transaction is being activated (starting on
     * the journal).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     * 
     * @todo test for transactions that have already been completed? that would
     *       represent a protocol error in the transaction manager service.
     */
    public void activateTx(final ITx tx) throws IllegalStateException {

        final Long timestamp = tx.getStartTimestamp();

        if (activeTx.containsKey(timestamp)) {

            throw new IllegalStateException("Already active: tx=" + tx);
            
        }

        if (preparedTx.containsKey(timestamp)) {

            throw new IllegalStateException("Already prepared: tx=" + tx);
            
        }

        activeTx.put(timestamp, tx);

    }

    /**
     * Notify the journal that a transaction has prepared (and hence is no
     * longer active).
     * 
     * @param tx
     *            The transaction
     * 
     * @throws IllegalStateException
     */
    public void preparedTx(final ITx tx) throws IllegalStateException {

        final Long startTime = tx.getStartTimestamp();

        final ITx tx2 = activeTx.remove(startTime);

        if (tx2 == null)
            throw new IllegalStateException("Not active: tx=" + tx);

        assert tx == tx2;

        if (preparedTx.containsKey(startTime)) {

            throw new IllegalStateException("Already preparing: tx=" + tx);
        
        }

        preparedTx.put(startTime, tx);

    }

    /**
     * Notify the journal that a transaction is completed (either aborted or
     * committed).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     */
    public void completedTx(final ITx tx) throws IllegalStateException {

        assert tx != null;
        assert tx.isComplete();

        final Long startTime = tx.getStartTimestamp();

        final ITx txActive = activeTx.remove(startTime);

        final ITx txPrepared = preparedTx.remove(startTime);

        if (txActive == null && txPrepared == null) {

            throw new IllegalStateException(
                    "Neither active nor being prepared: tx=" + tx);

        }

    }

    /**
     * Lookup an active or prepared transaction (exact match).
     * 
     * @param startTime
     *            The start timestamp for the transaction.
     * 
     * @return The transaction with that start time or <code>null</code> if
     *         the start time is not mapped to either an active or prepared
     *         transaction.
     */
    public ITx getTx(final long startTime) {

        // @todo lock to prevent concurrent decl or state change for the tx?
        
        ITx tx = activeTx.get(startTime);

        if (tx == null) {

            tx = preparedTx.get(startTime);

        }

        return tx;

    }

    /*
     * ITxCommitProtocol.
     */

//    /**
//     * FIXME modify to accept the desired start time for read-only transactions
//     * and to assign an actual start time for read-only transactions that is GTE
//     * to the most recent commit time that is NOT already in use by any active
//     * transaction and that is strictly LT the current time (probed without
//     * assignment). If necessary, this will cause the caller to block until a
//     * suitable timestamp is available.
//     * 
//     * This should be done by delegation to the core impl that is also used for
//     * the {@link IBigdataFederation} - the {@link TransactionService}.
//     */
//    public long newTx(final long timestamp) {
//
//        throw new UnsupportedOperationException();
//        
////        final ILocalTransactionManager transactionManager = this;
////
////        final long startTime = nextTimestampRobust();
////        
////        switch (level) {
////        
////        case ReadOnly: {
////
////            new Tx(transactionManager, resourceManager, startTime, true);
////
////            return startTime;
////        }
////
////        case ReadWrite: {
////            
////            new Tx(transactionManager, resourceManager, startTime, false);
////            
////            return startTime;
////
////        }
////
////        default:
////
////            throw new AssertionError("Unknown isolation level: " + level);
////        
////        }
//
//    }
    
    /**
     * Abort a transaction (synchronous, low latency for read-only transactions
     * but aborts for read-write transactions are serialized since there may be
     * latency in communications with the transaction server or deletion of the
     * temporary backing store for the transaction).
     * 
     * @param ts
     *            The transaction identifier (aka start time).
     */
    public void abort(final long ts) {

        final ITx tx = getTx(ts);

        if (tx == null) {

            throw new IllegalStateException("No such tx: " + ts);
            
        }

        // abort is synchronous.
        tx.abort();

        /*
         * Note: We do not need to abort the pending group commit since nothing
         * is written by the transaction on the unisolated indices until it has
         * validated - and the validate/merge task is an unisolated write
         * operation.
         */

    }

    /**
     * Commit a transaction (synchronous).
     * <p>
     * Read-only transactions and transactions without write sets are processed
     * immediately and will have a commit time of ZERO (0L).
     * <p>
     * Transactions with non-empty write sets are placed onto the
     * {@link WriteExecutorService} and the caller will block until the
     * transaction either commits or aborts. For this reason, this method MUST
     * be invoked from within a worker thread for the transaction so that
     * concurrent transactions may continue to execute.
     * 
     * @param ts
     *            The transaction identifier (aka start time).
     * 
     * @return The transaction commit time -or- ZERO (0L) if the transaction was
     *         read-only or had empty write sets.
     * 
     * @exception ValidationError
     *                If the transaction could not be validated. A transaction
     *                that can not be validated is automatically aborted. The
     *                caller MAY re-execute the transaction.
     */
    public long commit(final long ts) throws ValidationError {

        final ITx tx = getTx(ts);

        if (tx == null) {

            throw new IllegalStateException("No such tx: " + ts);

        }

        /*
         * A read-only transaction can commit immediately since validation and
         * commit are basically NOPs.
         */

        if (tx.isReadOnly()) {

            // read-only transactions do not get a commit time.
            tx.prepare(0L);

            return tx.commit();

        }

        /*
         * A transaction with an empty write set can commit immediately since
         * validation and commit are basically NOPs (this is the same as the
         * read-only case.)
         */

        if (tx.isEmptyWriteSet()) {

            tx.prepare(0L);

            return tx.commit();

        }

        final IConcurrencyManager concurrencyManager = getConcurrencyManager();
        
        try {
            
            final AbstractTask task = new TxCommitTask(concurrencyManager, tx);

            final Long commitTime = (Long) concurrencyManager.getWriteService()
                    .submit(task).get();

            if (DEBUG) {

                log.debug("committed: startTime=" + tx.getStartTimestamp()
                        + ", commitTime=" + commitTime);

            }

            return commitTime;

        } catch (InterruptedException ex) {

            // interrupted, perhaps during shutdown.
            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {

            final ValidationError cause = (ValidationError) InnerCause
                    .getInnerCause(ex, ValidationError.class);
            
            if(cause != null) {
                
                throw (ValidationError) cause;
                
            }
            
            // this is an unexpected error.
            throw new RuntimeException(ex);

        }

    }

//    public void wroteOn(long startTime, UUID dataServiceUUID) {
//        
//        /*
//         * NOP for a standalone journal.
//         */
//        
//    }
    
    /**
     * This task is an UNISOLATED operation that validates and commits a
     * transaction known to have non-empty write sets.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class TxCommitTask extends AbstractTask {

        /**
         * The transaction that is being committed.
         */
        private final ITx tx;

        public TxCommitTask(IConcurrencyManager concurrencyManager, ITx tx) {

            super(concurrencyManager, ITx.UNISOLATED, tx.getDirtyResource());

            this.tx = tx;

        }

        /**
         * 
         * @return The commit time assigned to the transaction.
         */
        public Object doTask() throws Exception {

            if(INFO)
                log.info("resource="+Arrays.toString(getResource()));
            
            /*
             * The commit time is assigned when we prepare the transaction.
             */

            final long commitTime = nextTimestamp();

            /*
             * @todo should I acquire AbstractTx.lock around the prepare/commit?
             */
            
            tx.prepare(commitTime);

            return Long.valueOf(tx.commit());

        }

    }

    public boolean isOpen() {
        
        return open;
        
    }
    
    private boolean open = true;
    
    synchronized public void shutdown() {
        
        // Note: currently a NOP.
        
        if(!open) return;
        
        open = false;
        
    }

    synchronized public void shutdownNow() {

        // Note: currently a NOP.
        
        if(!open) return;
        
        open = false;
        
    }

    /**
     * Delay between attempts reach the remote service (ms).
     */
    final long delay = 10L;
    
    /**
     * #of attempts to reach the remote service.
     * 
     * Note: delay*maxtries == 1000ms of trying before we give up.
     * 
     * If this is not enough, then consider adding an optional parameter giving
     * the time the caller will wait and letting the StoreManager wait longer
     * during startup to discover the timestamp service.
     */
    final int maxtries = 100; 
    
    /**
     * Note: The reason for all this retry logic is to work around race
     * conditions during service startup (and possibly during service failover)
     * when the {@link ITimestampService} has not been discovered yet.
     */
    public long nextTimestamp() {

        final long begin = System.currentTimeMillis();
        
        IOException cause = null;

        int ntries;

        for (ntries = 1; ntries <= maxtries; ntries++) {

            try {

                final ITransactionService transactionService = getTransactionService();

                if (transactionService == null) {

                    log.warn("Service not discovered yet?");

                    try {

                        Thread.sleep(delay/* ms */);

                        continue;
                        
                    } catch (InterruptedException e2) {

                        throw new RuntimeException(
                                "Interrupted awaiting timestamp service discovery: "
                                        + e2);

                    }

                }

                return transactionService.nextTimestamp();
                
            } catch (IOException e) {

                log.warn("Problem with timestamp service? : ntries=" + ntries
                        + ", " + e, e);

                cause = e;

            }

        }

        final long elapsed = System.currentTimeMillis() - begin;

        final String msg = "Could not get timestamp after " + ntries
                + " tries and " + elapsed + "ms";

        log.error(msg, cause);

        throw new RuntimeException(msg, cause);
        
    }

    public void notifyCommit(final long commitTime) {
        
        final long begin = System.currentTimeMillis();
        
        IOException cause = null;

        int ntries;

        for (ntries = 1; ntries <= maxtries; ntries++) {

            try {

                final ITransactionService transactionService = getTransactionService();

                if (transactionService == null) {

                    log.warn("Service not discovered?");

                    try {

                        Thread.sleep(delay/* ms */);

                    } catch (InterruptedException e2) {

                        throw new RuntimeException(
                                "Interrupted awaiting timestamp service discovery: "
                                        + e2);

                    }

                    continue;

                }

                transactionService.notifyCommit(commitTime);

                return;

            } catch (IOException e) {

                log.warn("Problem with timestamp service? : ntries=" + ntries
                        + ", " + e, e);

                cause = e;

            }

        }

        final long elapsed = System.currentTimeMillis() - begin;

        final String msg = "Could not notify timestamp service after " + ntries
                + " tries and " + elapsed + "ms";

        log.error(msg, cause);

        throw new RuntimeException(msg, cause);
        
    }
    
    /**
     * Return interesting statistics about the transaction manager.
     */
    synchronized public CounterSet getCounters() {
        
        if (countersRoot == null) {

            countersRoot = new CounterSet();

            countersRoot.addCounter("#active", new Instrument<Integer>() {
                protected void sample() {
                    setValue(activeTx.size());
                }
            });

            countersRoot.addCounter("#prepared", new Instrument<Integer>() {
                protected void sample() {
                    setValue(preparedTx.size());
                }
            });

        }
        
        return countersRoot;
        
    }
    private CounterSet countersRoot;
    
}
