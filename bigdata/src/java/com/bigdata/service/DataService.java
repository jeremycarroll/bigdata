/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Mar 14, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.IBatchOperation;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.btree.IReadOnlyOperation;
import com.bigdata.btree.IndexSegment;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrentJournal;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.scaleup.JournalMetadata;
import com.bigdata.scaleup.ResourceState;

/**
 * An implementation of a network-capable {@link IDataService}. The service is
 * started using the {@link DataServer} class. Operations are submitted using
 * {@link ConcurrentJournal#submit(AbstractTask)} and will run with the
 * appropriate concurrency controls as imposed by that method.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see DataServer, which is used to start this service.
 * 
 * @see NIODataService, which contains some old code that can be refactored for
 *      an NIO interface to the data service.
 * 
 * @todo consider further refactoring of the operations exposed here into the
 *       journal package and examine how to refactor the RDFS database so as to
 *       play nice with the {@link ConcurrentJournal}.
 * 
 * @todo Support overflow. Queued tasks should be migrated from the "old"
 *       journal to the "new" journal while running tasks should complete on the
 *       "old" journal. Consider encapsulating this behavior in a base class
 *       using a delegation model. There is a sketch of that kind of a thing in
 *       the "scaleout" package. The specifics should probably be discarded but
 *       parts of the code may be of use. The handling of overflow events needs
 *       to be coordinated with the {@link IMetadataService}.
 *       <p>
 *       MVCC requires a strategy to release old versions that are no longer
 *       accessible to active transactions. bigdata uses a highly efficient
 *       technique in which writes are multiplexed onto append-only
 *       {@link Journal}s and then evicted on overflow into
 *       {@link IndexSegment}s using a bulk index build mechanism. Old journal
 *       and index segment resources are simply deleted from the file system
 *       some time after they are no longer accessible to active transactions.
 * 
 * @todo The data service should redirect clients if an index partition has been
 *       moved (shed) while a client has a lease.
 * 
 * @todo Support GOM pre-fetch using a rangeQuery iterator - that will
 *       materialize N records on the client and could minimize trips to the
 *       server. I am not sure about unisolated operations for GOM.... Isolated
 *       operations are straight forward. The other twist is supporting scalable
 *       link sets, link set indices (not named, unless the identity of the
 *       object collecting the link set is part of the key), and non-OID indices
 *       (requires changes to generic-native). I think that link sets might have
 *       to become indices in order to scale (to break the cycle of updating
 *       both the object collecting the link set and the head/tail and
 *       prior/next members in the link set). Or perhaps all those could be
 *       materialized on the client and then an unisolated operation (perhaps
 *       with conflict resolution?!?) would persist the results...
 * 
 * @todo Participate in 1-phase (local) and 2-/3- phrase (distributed) commits
 *       with an {@link ITransactionManager} service. The data service needs to
 *       notify the {@link ITransactionManager} each time an isolated writer
 *       touches a named index so that the transaction manager can build up the
 *       set of resources that must be locked during the validate/commit phrase.
 * 
 * @todo Write benchmark test to measure interhost transfer rates. Should be
 *       100Mbits/sec (~12M/sec) on a 100BaseT switched network. With full
 *       duplex in the network and the protocol, that rate should be
 *       bidirectional. Can that rate be sustained with a fully connected
 *       bi-directional transfer?
 * 
 * @todo Review JERI options to support non-blocking/fast RMI protocols. See
 *       <p>
 *       http://archives.java.sun.com/cgi-bin/wa?A2=ind0504&L=jini-users&P=33490<br>
 *       http://archives.java.sun.com/cgi-bin/wa?A2=ind0506&L=jini-users&P=9626<br>
 *       http://archives.java.sun.com/cgi-bin/wa?A2=ind0504&L=jini-users&D=0&P=26542<br>
 *       http://java.sun.com/products/jini/2.0.1/doc/api/net/jini/jeri/tcp/package-summary.html<br>
 *       <p>
 *       Try net.jini.jeri.tcp endpoints without socket factories and set the
 *       "com.sun.jini.jeri.tcp.useNIO" to "true". JRMP is evidentally much
 *       slower since it does not allow for the possibility of NIO.
 * 
 * @todo Review JERI options to support secure RMI protocols. For example, using
 *       SSL or an SSH tunnel. For most purposes I expect bigdata to operate on
 *       a private network, but replicate across gateways is also a common use
 *       case. Do we have to handle it specially?
 * 
 * @todo Keep the "wire" format for the data and metadata services as clean as
 *       possible so that it will be possible for non-Java clients to talk to
 *       these services (assuming that they can talk to Jini...).
 */
abstract public class DataService implements IDataService,
        IWritePipeline, IResourceTransfer, IServiceShutdown {

    protected Journal journal;

    public static final Logger log = Logger.getLogger(DataService.class);

    /**
     * Options understood by the {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Options extends com.bigdata.journal.Options {
        
    }
    
    /**
     * 
     * @param properties
     */
    public DataService(Properties properties) {
        
        journal = new DataServiceJournal(properties);

    }

    /**
     * Polite shutdown does not accept new requests and will shutdown once
     * the existing requests have been processed.
     */
    public void shutdown() {
        
        journal.shutdown();
        
    }
    
    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon
     * as possible.
     */
    public void shutdownNow() {

        journal.shutdownNow();
        
    }

    /**
     * The unique identifier for this data service.
     * 
     * @return The unique data service identifier.
     */
    public abstract UUID getServiceUUID() throws IOException;
    
    /*
     * ITxCommitProtocol.
     */
    
    public long commit(long tx) throws IOException {
        
        setupLoggingContext();
        
        try {
        
            // will place task on writeService and block iff necessary.
            return journal.commit(tx);
        
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public void abort(long tx) throws IOException {

        setupLoggingContext();

        try {

            // will place task on writeService iff read-write tx.
            journal.abort(tx);
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    /*
     * IDataService.
     */
    
    /**
     * Forms the name of the index corresponding to a partition of a named
     * scale-out index as <i>name</i>#<i>partitionId</i>.
     * <p>
     * Another advantage of this naming scheme is that index partitions are just
     * named indices and all of the mechanisms for operating on named indices
     * and for concurrency control for named indices apply automatically. Among
     * other things, this means that different tasks can write concurrently on
     * different partitions of the same named index on a given
     * {@link DataService}.
     * 
     * @return The name of the index partition.
     */
    public static final String getIndexPartitionName(String name,
            int partitionId) {

        if (name == null) {

            throw new IllegalArgumentException();
            
        }

        return name + "#" + partitionId;

    }
    
    /**
     * Return true iff the value identifies a read-only transaction known to
     * this data service.
     * 
     * @param startTime
     *            The transaction identifier.
     * 
     * @return True iff that is a read-only transaction.
     * 
     * @exception IllegalStateException
     *                if the transaction identifier is not active.
     */
    private boolean isReadOnly(long startTime) {
        
        assert startTime != 0l;
        
        ITx tx = journal.getTx(startTime);
        
        if (tx == null) {

            throw new IllegalStateException("Unknown: tx=" + startTime);
            
        }
        
        return tx.isReadOnly();
        
    }

    /**
     * @todo if the journal overflows then the returned metadata can become
     *       stale (the journal in question will no longer be absorbing writes
     *       but it will continue to be used to absorb reads until the asyn
     *       overflow operation is complete, at which point the journal can be
     *       closed. the journal does not become "Dead" until it is no longer
     *       possible that a live transaction will want to read from a
     *       historical state found on that journal).
     */
    public JournalMetadata getJournalMetadata() throws IOException {
        
        return new JournalMetadata(journal,ResourceState.Live);
        
    }

    /**
     * Sets up the {@link MDC} logging context. You should do this on every
     * client facing point of entry and then call {@link #clearLoggingContext()}
     * in a <code>finally</code> clause. You can extend this method to add
     * additional context.
     * <p>
     * This implementation add the "serviceUUID" parameter to the {@link MDC}.
     * The serviceUUID is, in general, assigned asynchronously by the service
     * registrar. Once the serviceUUID becomes available it will be added to the
     * {@link MDC}. This datum can be injected into log messages using
     * %X{serviceUUID} in your log4j pattern layout.
     */
    protected void setupLoggingContext() {

        try {
            
            // Note: This _is_ a local method call.
            
            UUID serviceUUID = getServiceUUID();
            
            // Will be null until assigned by the service registrar.
            
            if (serviceUUID == null) {

                return;
                
            }
            
            // Add to the logging context for the current thread.
            
            MDC.put("serviceUUID", serviceUUID.toString());

        } catch(Throwable t) {
            /*
             * Ignore.
             */
        }
        
    }

    /**
     * Clear the logging context.
     */
    protected void clearLoggingContext() {
        
        MDC.remove("serviceUUID");
        
    }

    public void registerIndex(String name, UUID indexUUID, String className,
            Object config) throws IOException, InterruptedException,
            ExecutionException {

        setupLoggingContext();
        
        try {
            
            if (indexUUID == null)
                throw new IllegalArgumentException();
    
            if( className == null)
                throw new IllegalArgumentException();
            
            final Class cls;
            
            try {
    
                cls = Class.forName(className);
                
            } catch(Exception ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            if (!BTree.class.isAssignableFrom(cls)) {
    
                throw new IllegalArgumentException("Class does not extend: "
                        + BTree.class);
                
            }
            
            final BTree btree;
            
            if(cls.equals(UnisolatedBTree.class)) {
    
                btree = new UnisolatedBTree(journal, indexUUID);
                
            } else if(cls.equals(UnisolatedBTreePartition.class)) {
    
                UnisolatedBTreePartition.Config tmp = (UnisolatedBTreePartition.Config) config;
    
                btree = new UnisolatedBTreePartition(journal, indexUUID, tmp);
                
            } else {
                
                /*
                 * @todo Don't know how to configure this kind of btree.  The
                 * registration should be based on the configuration object.
                 */
                throw new UnsupportedOperationException();
                
            }

//        journal.submit(new RegisterIndexTask(journal, name, btree)).get();
        
            journal.registerIndex(name, btree);
        
        } finally {
            
            clearLoggingContext();
            
        }

    }
    
    public void dropIndex(String name) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {
        
//      journal.submit(new DropIndexTask(journal, name)).get();

            journal.dropIndex(name);
        
        } finally {
            
            clearLoggingContext();
            
        }

    }
    
    public UUID getIndexUUID(String name) throws IOException {

        setupLoggingContext();
        
        try {

            final IIndex ndx = journal.getIndex(name);
            
            if(ndx == null) {
                
                return null;
                
            }
            
            return ndx.getIndexUUID();
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    /**
     * @todo modify to allow vals[] as null when index does not use values to
     *       save on network IO.
     */
    public byte[][] batchInsert(long tx, String name, int partitionId, int ntuples,
            byte[][] keys, byte[][] vals, boolean returnOldValues)
            throws IOException, InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            BatchInsert op = new BatchInsert(ntuples, keys, vals);
    
            batchOp(tx, name, partitionId, op);
    
            return returnOldValues ? (byte[][]) op.values : null;

        } finally {
            
            clearLoggingContext();
            
        }
            
    }

    public boolean[] batchContains(long tx, String name, int partitionId, int ntuples,
            byte[][] keys) throws IOException, InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            BatchContains op = new BatchContains(ntuples, keys, new boolean[ntuples]);
            
            batchOp( tx, name, partitionId, op );
    
            return op.contains;
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }
    
    public byte[][] batchLookup(long tx, String name, int partitionId, int ntuples, byte[][] keys)
            throws IOException, InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            BatchLookup op = new BatchLookup(ntuples,keys,new byte[ntuples][]);
            
            batchOp(tx, name, partitionId, op);
            
            return (byte[][])op.values;

        } finally {
            
            clearLoggingContext();
            
        }

    }
    
    public byte[][] batchRemove(long tx, String name, int partitionId, int ntuples,
            byte[][] keys, boolean returnOldValues)
            throws IOException, InterruptedException, ExecutionException {
        
        setupLoggingContext();
        
        try {
        
            BatchRemove op = new BatchRemove(ntuples,keys,new byte[ntuples][]);
            
            batchOp(tx, name, partitionId, op);
            
            return returnOldValues ? (byte[][])op.values : null;
        
        } finally {
            
            clearLoggingContext();
            
        }

    }
    
    /**
     * Executes a batch operation on a named btree.
     * 
     * @param tx
     *            The transaction identifier -or- zero (0L) IFF the operation is
     *            NOT isolated by a transaction.
     * @param name
     *            The index name (required).
     * @param op
     *            The batch operation.
     * 
     * @exception InterruptedException
     *                if the operation was interrupted (typically by
     *                {@link #shutdownNow()}.
     * @exception ExecutionException
     *                If the operation caused an error. See
     *                {@link ExecutionException#getCause()} for the underlying
     *                error.
     */
    protected void batchOp(long tx, String name, int partitionId, IBatchOperation op)
            throws InterruptedException, ExecutionException {
        
        if( name == null ) throw new IllegalArgumentException();
        
        if( op == null ) throw new IllegalArgumentException();
        
        final boolean isolated = tx != 0L;
        
        final boolean readOnly = (op instanceof IReadOnlyOperation)
                || (isolated && isReadOnly(tx));

        // submit the task and wait for it to complete.

        journal.submit(new BatchTask(journal, tx, readOnly, DataService
                .getIndexPartitionName(name, partitionId), op)).get();
        
    }    
    
    public Object submit(long tx, String name, int partitionId, IProcedure proc)
            throws InterruptedException, ExecutionException {

        setupLoggingContext();

        try {
            
            if (proc == null)
                throw new IllegalArgumentException();
    
            final boolean readOnly = proc instanceof IReadOnlyOperation;
    
            // submit the procedure and await its completion.
    
            return journal.submit(
                    new ProcedureTask(journal, tx, readOnly, DataService
                            .getIndexPartitionName(name, partitionId), proc)).get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }

    public int rangeCount(long tx, String name, int partitionId,
            byte[] fromKey, byte[] toKey) throws InterruptedException,
            ExecutionException {

        setupLoggingContext();
        
        try {

            final RangeCountTask task = new RangeCountTask(journal, tx, DataService
                .getIndexPartitionName(name, partitionId), fromKey, toKey);

            // submit the task and wait for it to complete.
            
            return (Integer) journal.submit(task).get();

        } finally {
            
            clearLoggingContext();
            
        }

    }
    
    /**
     * 
     * @todo the iterator needs to be aware of the defintion of a "row" for the
     *       sparse row store so that we can respect the atomic guarentee for
     *       reads as well as writes.
     * 
     * @todo support filters. there are a variety of use cases from clients that
     *       are aware of version counters and delete markers to clients that
     *       encode a column name and datum or write time into the key to those
     *       that will filter based on inspection of the value associated with
     *       the key, e.g., only values having some attribute.
     * 
     * @todo if we allow the filter to cause mutations (e.g., deleting matching
     *       entries) then we have to examine the operation to determine whether
     *       or not we need to use the {@link #txService} or the
     *       {@link #readService}
     */
    public ResultSet rangeQuery(long tx, String name, int partitionId,
            byte[] fromKey, byte[] toKey, int capacity, int flags)
            throws InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            if( name == null ) throw new IllegalArgumentException();
            
            final RangeQueryTask task = new RangeQueryTask(journal, tx, DataService
                    .getIndexPartitionName(name, partitionId), fromKey, toKey,
                    capacity, flags);
    
            // submit the task and wait for it to complete.
            return (ResultSet) journal.submit(task).get();
        
        } finally {
            
            clearLoggingContext();
            
        }
            
    }

    /**
     * Abstract class for tasks that execute batch api operations. There are
     * various concrete subclasses, each of which MUST be submitted to the
     * appropriate service for execution.
     * <p>
     * Note: While this does verify that the first/last key are inside of the
     * specified index partition, it does not verify that the keys are sorted -
     * this is the responsibility of the client. Therefore it is possible that
     * an incorrect client providing unsorted keys could execute an operation
     * that read or wrote data on the data service that lay outside of the
     * indicated partitionId.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class BatchTask extends AbstractTask {
        
        private final IBatchOperation op;
        
        public BatchTask(ConcurrentJournal journal, long startTime,
                boolean readOnly, String name, IBatchOperation op) {

            super(journal, startTime, readOnly, name);
            
            if (op == null)
                throw new IllegalArgumentException();
            
            this.op = op;
            
        }
        
        final protected Object doTask() throws Exception {
        
            IIndexWithCounter ndx = getIndex(getOnlyResource());
            
            final int ntuples = op.getTupleCount();
            
            final byte[][] keys = op.getKeys();

            if(ndx instanceof UnisolatedBTreePartition) {

                /*
                 * If this is an index partition, then test the keys against the
                 * separator keys for the partition. All client keys must lie
                 * within the partition ( left <= key < right ).
                 * 
                 * @todo this is not verifying the partition unless we are doing
                 * a write since otherwise we do not see the
                 * UnisolatedBTreePartition class directly but rather some
                 * wrapper object.
                 */
             
                ((UnisolatedBTreePartition)ndx).rangeCheck(keys[0]);
                
                ((UnisolatedBTreePartition)ndx).rangeCheck(keys[ntuples-1]);

            }
            
            if( op instanceof BatchContains ) {

                ndx.contains((BatchContains) op);
                
            } else if( op instanceof BatchLookup ) {

                ndx.lookup((BatchLookup) op);

            } else if( op instanceof BatchInsert ) {

                ndx.insert((BatchInsert) op);

            } else if( op instanceof BatchRemove ) {

                ndx.remove((BatchRemove) op);

            } else {

                /*
                 * Extension batch mutation operation.
                 */ 

                op.apply(ndx);
                
            }
            
            return null;
            
        }
        
    }

    /**
     * Task for running a rangeCount operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class RangeCountTask extends AbstractTask {

        private final byte[] fromKey;
        private final byte[] toKey;
        
        public RangeCountTask(ConcurrentJournal journal,long startTime, String name,
                byte[] fromKey, byte[] toKey) {
            
            super(journal,startTime,true/*readOnly*/,name);
            
            this.fromKey = fromKey;
            this.toKey = toKey;
            
        }

        public Object doTask() throws Exception {
            
            return new Integer(getIndex(getOnlyResource()).rangeCount(fromKey, toKey));
            
        }
        
    }

    /**
     * Task for running a rangeQuery operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class RangeQueryTask extends AbstractTask {

        private final byte[] fromKey;
        private final byte[] toKey;
        private final int capacity;
        private final int flags;
        
        public RangeQueryTask(ConcurrentJournal journal, long startTime,
                String name, byte[] fromKey, byte[] toKey, int capacity,
                int flags) {

            super(journal,startTime,true/*readOnly*/,name);
            
            this.fromKey = fromKey;
            this.toKey = toKey;
            this.capacity = capacity;
            this.flags = flags;
            
        }
        
        public Object doTask() throws Exception {
            
            final boolean sendKeys = (flags & KEYS) != 0;
            
            final boolean sendVals = (flags & VALS) != 0;
            
            return new ResultSet(getIndex(getOnlyResource()), fromKey, toKey,
                    capacity, sendKeys, sendVals);
            
        }
        
    }

    /**
     * Abstract class for tasks that execute {@link IProcedure} operations.
     * There are various concrete subclasses, each of which MUST be submitted to
     * the appropriate service for execution.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class ProcedureTask extends AbstractTask {
        
        protected final IProcedure proc;
        
        public ProcedureTask(ConcurrentJournal journal, long startTime,
                boolean readOnly, String name, IProcedure proc) {

            super(journal,startTime, readOnly, name);
            
            if (proc == null)
                throw new IllegalArgumentException();
            
            this.proc = proc;
            
        }
        
        final public Object doTask() throws Exception {

            return proc.apply(getIndex(getOnlyResource()));

        }

    }

    /**
     * @todo IResourceTransfer is not implemented.
     */
    public void sendResource(String filename, InetSocketAddress sink) {
        throw new UnsupportedOperationException();
    }
    
    /**
     * 
     * FIXME Either this class or its outer class MUST discover the
     * {@link ITransactionManager} and delegate the methods on that interface to
     * that service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class DataServiceJournal extends Journal {

        /**
         * @param properties
         */
        public DataServiceJournal(Properties properties) {
            
            super(properties);
            
        }
        
        /**
         * Returns instance of {@link StatusTask}.
         */
        protected StatusTask newStatusTask() {

            return new StatusTask();
            
        }

        /**
         * Extended to add the logging context.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        protected class StatusTask extends ConcurrentJournal.StatusTask {
            
            protected void status() {
                
                setupLoggingContext();
                
                try {
                    
                    super.status();
                    
                } finally {
                    
                    clearLoggingContext();
                    
                }
                
            }
            
        }
        
    }
    
}
