/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 24, 2008
 */

package com.bigdata.relation.rule.eval;

import java.io.IOException;
import java.util.Iterator;
//BTM - PRE_FRED_3481 import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.ChunkConsumerIterator;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
//BTM - PRE_FRED_3481 import com.bigdata.service.DataService;
//BTM - PRE_FRED_3481 import com.bigdata.service.DataServiceCallable;
//BTM - FOR_CLIENT_SERVICE import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;

//BTM
//BTM - PRE_FRED_3481 import com.bigdata.resources.LocalResourceManagement;
//BTM - PRE_FRED_3481 import com.bigdata.service.ShardService;
//BTM - POST_FRED_3481
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.IDataServiceCallable;
import com.bigdata.service.Session;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.journal.TransactionService;

/**
 * Task for executing a program when all of the indices for the relation are
 * co-located on the same {@link DataService}.
 *
 * @todo Named result sets. This would provide a means to run a IRuleTask and
 *       cache the output for further evaluation as a named result set. The
 *       backing store should be a temporary resource. for scale-out it needs to
 *       be visible to the federation (since the rule executing against that
 *       data may be distributed across the federation based on the access path
 *       for the SPORelation) so it would have to be registered on some data
 *       service (any) in the federation and dropped in a finally {} clause.
 *       <p>
 *       When the sets are large then they may need a backing store, e.g.,
 *       BigdataSet<Long> (specialized so that it does not store anything under
 *       the key since we can decode the Long from the key - do utility versions
 *       BigdataLongSet(), but the same code can serve float, double, and int as
 *       well. Avoid override for duplicate keys to reduce IO.
 *
 * @todo it should be possible to have a different action associated with each
 *       rule in the program, and to have a different target relation for the
 *       head of each rule on which we will write (mutation). Different query or
 *       mutation count results could be handled by an extension with
 *       "nextResultSet" style semantics. However, for now, all rules MUST write
 *       on the same buffer. Query results will therefore be multiplexed as will
 *       mutations counts.
 *
 * @todo foreign key joins: it should be possible to handle different relation
 *       classes in the same rules, e.g., RDF and non-RDF relations. Or even the
 *       SPO and lexicon relation for the RDF DB -- the latter will be useful
 *       for materializing externalized statements efficiently.
 *
 * @todo could make the return type a generic for {@link AbstractStepTask} and
 *       make this class a concrete implementation of that one.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
//BTM - PRE_FRED_3481 public class ProgramTask extends DataServiceCallable<Object> implements IProgramTask {
public class ProgramTask implements IDataServiceCallable<Object> {

    /**
     *
     */
    private static final long serialVersionUID = -7047397038429305180L;

    protected static final transient Logger log = Logger.getLogger(ProgramTask.class);

    private final ActionEnum action;

    private final IStep step;

    /**
     * Serializable field specified when the {@link ProgramTask} will be
     * submitted (via RMI or not) to a {@link DataService}. A new
     * {@link IJoinNexus} is instantiated in the execution context on the
     * {@link DataService} from this field.
     */
    private final IJoinNexusFactory joinNexusFactory;
    
//BTM - PRE_FRED_3481    /**
//BTM - PRE_FRED_3481     * Note: NOT serialized! The {@link IIndexManager} will be set by
//BTM - PRE_FRED_3481     * {@link #setDataService(DataService)} if this object submitted using
//BTM - PRE_FRED_3481     * {@link DataService#submit(Callable)}.
//BTM - PRE_FRED_3481     */
//BTM - PRE_FRED_3481    private transient IIndexManager indexManager;
//BTM - PRE_FRED_3481    
//BTM - PRE_FRED_3481//    /**
//BTM - PRE_FRED_3481//     * Note: NOT serialized!
//BTM - PRE_FRED_3481//     */
//BTM - PRE_FRED_3481//    private transient DataService dataService;
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481    @Override
//BTM - PRE_FRED_3481//BTM    public void setDataService(final DataService dataService) {
//BTM - PRE_FRED_3481public void setDataService(final ShardService dataService) {
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481        super.setDataService(dataService);
//BTM - PRE_FRED_3481        
//BTM - PRE_FRED_3481//BTM        this.indexManager = dataService.getFederation();
//BTM - PRE_FRED_3481if(dataService instanceof DataService) {
//BTM - PRE_FRED_3481    this.indexManager = ((DataService)dataService).getFederation();
//BTM - PRE_FRED_3481} else {//EmbeddedShardService
//BTM - PRE_FRED_3481    this.indexManager = ((LocalResourceManagement)dataService).getIndexManager();
//BTM - PRE_FRED_3481}
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481    }

    /**
     * Variant when the task will be submitted using
     * {@link IDataService#submit(IDataServiceCallable)} (efficient since all indices will
     * be local, but the indices must not be partitioned and must all exist on
     * the target {@link DataService}).
     * <p>
     * Note: the caller MUST submit the {@link ProgramTask} using
     * {@link DataService#submit(IDataServiceCallable)} in which case {@link #dataService}
     * field will be set (after the ctor) by the {@link DataService} itself. The
     * {@link DataService} will be used to identify an {@link ExecutorService}
     * and the {@link IJoinNexusFactory} will be used to establish access to
     * indices, relations, etc. in the context of the {@link AbstractTask} - see
     * {@link AbstractStepTask#submit()}.
     *
     * @param action
     * @param step
     * @param joinNexus
     */
    public ProgramTask(final ActionEnum action, final IStep step,
            final IJoinNexusFactory joinNexusFactory) {

        if (action == null)
            throw new IllegalArgumentException();

        if (step == null)
            throw new IllegalArgumentException();

        if (joinNexusFactory == null)
            throw new IllegalArgumentException();

        this.action = action;

        this.step = step;

        this.joinNexusFactory = joinNexusFactory;
        
//BTM - PRE_FRED_3481        this.indexManager = null; 

    }

//BTM - PRE_FRED_3481    /**
//BTM - PRE_FRED_3481     * Variant when the task will be executed directly by the caller.
//BTM - PRE_FRED_3481     *
//BTM - PRE_FRED_3481     * @param action
//BTM - PRE_FRED_3481     * @param step
//BTM - PRE_FRED_3481     * @param joinNexusFactory
//BTM - PRE_FRED_3481     * @param indexManager
//BTM - PRE_FRED_3481     *
//BTM - PRE_FRED_3481     * @throws IllegalArgumentException
//BTM - PRE_FRED_3481     *             if any parameter is <code>null</code>.
//BTM - PRE_FRED_3481     */
//BTM - PRE_FRED_3481    public ProgramTask(final ActionEnum action, final IStep step,
//BTM - PRE_FRED_3481            final IJoinNexusFactory joinNexusFactory, final IIndexManager indexManager) {
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481        if (action == null)
//BTM - PRE_FRED_3481            throw new IllegalArgumentException();
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481        if (step == null)
//BTM - PRE_FRED_3481            throw new IllegalArgumentException();
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481        if (joinNexusFactory == null)
//BTM - PRE_FRED_3481            throw new IllegalArgumentException();
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481        if (indexManager == null)
//BTM - PRE_FRED_3481            throw new IllegalArgumentException();
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481        this.action = action;
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481        this.step = step;
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481        this.joinNexusFactory = joinNexusFactory;
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481        this.indexManager = indexManager;
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481    }

    /**
     * Execute the program.
     * <p>
     * Note: There is no natural order for high-level query. Also, unless stable
     * evaluation is requested, the results can be produced by parallel threads
     * and the order of the materialized solution is therefore not even stable.
     * The only way to have a natural order is for a sort to be imposed on the
     * {@link ISolution}s.
     *
     * @throws Exception
     */
//BTM - PRE_FRED_3481    public Object call() throws Exception {
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public Object startDataTask(IIndexManager indexManager,
//BTM - PRE_CLIENT_SERVICE                                ResourceManager resourceManager,
//BTM - PRE_CLIENT_SERVICE                                IConcurrencyManager concurrencyManager,
//BTM - PRE_CLIENT_SERVICE                                Session session,
//BTM - PRE_CLIENT_SERVICE                                String hostName,
//BTM - PRE_CLIENT_SERVICE                                String serviceName) throws Exception {
//BTM - PRE_CLIENT_SERVICE
    public Object startDataTask(IIndexManager indexManager,
                                ResourceManager resourceManager,
                                IConcurrencyManager concurrencyManager,
                                ILocalResourceManagement localResources,
                                IBigdataDiscoveryManagement discoveryManager)
                      throws Exception
    {
//BTM - PRE_CLIENT_SERVICE - END

        if (log.isDebugEnabled()) {

            log.debug("begin: program=" + step.getName() + ", action="
                            + action);

        }

        try {

            final ProgramUtility util = new ProgramUtility();

            if (action.isMutation()) {

                final RuleStats totals;

                if (!step.isRule() && ((IProgram) step).isClosure()) {

                    /*
                     * Compute closure of a flat set of rules.
                     */

//BTM - PRE_FRED_3481       totals = executeClosure((IProgram) step);
//BTM - PRE_CLIENT_SERVICE  totals = executeClosure((IProgram) step, indexManager, concurrencyManager);
                    totals = executeClosure((IProgram) step,
                                             indexManager,
                                             concurrencyManager,
                                             discoveryManager);
                } else if (util.isClosureProgram(step)) {

                    /*
                     * Compute closure of a program that embedded closure
                     * operations.
                     */

//BTM - PRE_FRED_3481       totals = executeProgramWithEmbeddedClosure((IProgram)step);
//BTM - PRE_CLIENT_SERVICE  totals = executeProgramWithEmbeddedClosure((IProgram)step, indexManager, concurrencyManager);
                    totals = executeProgramWithEmbeddedClosure
                                 ( (IProgram)step,
                                   indexManager,
                                   concurrencyManager,
                                   discoveryManager );

                } else {

                    /*
                     * Execute a mutation operation that does not use closure.
                     */

//BTM - PRE_FRED_3481       totals = executeMutation(step);
//BTM - PRE_CLIENT_SERVICE  totals = executeMutation(step, indexManager, concurrencyManager);
                    totals = executeMutation(step,
                                             indexManager,
                                             concurrencyManager,
                                             discoveryManager);
                }

                RuleLog.log(totals);

                return totals.mutationCount.get();

            } else {

                if ((!step.isRule() && ((IProgram) step).isClosure())
                        || util.isClosureProgram(step)) {

                    /*
                     * The step is either a closure program or embeds a closure
                     * program.
                     */

                    throw new UnsupportedOperationException(
                            "Closure only allowed for mutation.");

                }

                /*
                 * Execute a query.
                 */
//BTM - PRE_FRED_3481       return new ChunkConsumerIterator<ISolution>(executeQuery(step));
//BTM - PRE_CLIENT_SERVICE  return new ChunkConsumerIterator<ISolution>( executeQuery(step, indexManager, concurrencyManager) );
                return new ChunkConsumerIterator<ISolution>
                               ( executeQuery(step,
                                              indexManager,
                                              concurrencyManager,
                                              discoveryManager) );
            }

        } finally {

            if (log.isDebugEnabled())
                log.debug("bye");

        }

    }

    /**
     * Execute the {@link IStep} as a query.
     *
     * @param step
     *            The {@link IStep}.
     *
     * @return The {@link IChunkedOrderedIterator} that will drain the
     *         {@link ISolution}s generated by the {@link IStep}. Execution
     *         will be cancelled if the iterator is
     *         {@link ICloseableIterator#close() closed}. If execution results
     *         in an error, then the iterator will throw a
     *         {@link RuntimeException} whose cause is the error.
     *
     * @throws RuntimeException
     */
//BTM - PRE_FRED_3481       protected IAsynchronousIterator<ISolution[]> executeQuery(final IStep step) {
    protected IAsynchronousIterator<ISolution[]> executeQuery
                  (final IStep step,
                   IIndexManager indexManager,
                   IConcurrencyManager concurrencyManager,
                   IBigdataDiscoveryManagement discoveryManager)
    {
        if (step == null)
            throw new IllegalArgumentException();

        if (log.isDebugEnabled())
            log.debug("program=" + step.getName());

        // buffer shared by all rules run in this query.
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE  final IBlockingBuffer<ISolution[]> buffer =
//BTM - PRE_CLIENT_SERVICE        joinNexusFactory.newInstance(indexManager).newQueryBuffer();
//BTM - PRE_CLIENT_SERVICE
        final IBlockingBuffer<ISolution[]> buffer =
            joinNexusFactory.newInstance(indexManager,
                                         concurrencyManager,
                                         discoveryManager).newQueryBuffer();
//BTM - PRE_CLIENT_SERVICE - END

        // the task to execute.
//BTM                  final QueryTask queryTask = new QueryTask(step, joinNexusFactory, buffer, indexManager, isDataService()?getDataService():null);
//BTM - PRE_FRED_3481  final QueryTask queryTask = new QueryTask(step, joinNexusFactory, buffer, indexManager, getResourceManager(), getConcurrencyManager(), getIndexManager(), getSession(), getHostname(), getServiceName());

//BTM - PRE_CLIENT_SERVICE        final QueryTask queryTask = new QueryTask(step, joinNexusFactory, buffer, indexManager);
        final QueryTask queryTask = new QueryTask(step, joinNexusFactory, buffer, indexManager, concurrencyManager, discoveryManager);

        Future<RuleStats> future = null;

        try {

            /*
             * Note: We do NOT get() this Future. This task will run
             * asynchronously.
             *
             * The Future is canceled IF (hopefully WHEN) the iterator is
             * closed.
             *
             * If the task itself throws an error, then it will use
             * buffer#abort(cause) to notify the buffer of the cause (it will be
             * passed along to the iterator) and to close the buffer (the
             * iterator will notice that the buffer has been closed as well as
             * that the cause was set on the buffer).
             *
             * @todo if the #of results is small and they are available with
             * little latency then return the results inline using a fully
             * buffered iterator.
             */

            // run the task.
//BTM - PRE_FRED_3481            future = queryTask.submit();
            future = queryTask.submit(concurrencyManager);

            // set the future on the BlockingBuffer.
            buffer.setFuture(future);

            if (log.isDebugEnabled())
                log.debug("Returning iterator reading on async query task");

            // return the async iterator.
            return buffer.iterator();

        } catch (Throwable ex) {

            try {

                log.error(ex, ex);

                throw new RuntimeException(ex);

            } finally {

                buffer.close();

                if (future != null) {

                    future.cancel(true/* mayInterruptIfRunning */);

                }

            }

        }

    }

    /**
     * Run a mutation {@link IStep}. The {@link IStep} may consist of many sub-{@link IStep}s.
     * <p>
     * Note: If you specify {@link ITx#READ_COMMITTED} for mutation operations
     * when using a federation then concurrent split/join/move can cause the
     * operation to fail. It is safer to use read-consistent semantics by
     * specifying {@link IIndexStore#getLastCommitTime()} instead.
     *
     * @param step
     *            The {@link IStep}.
     *
     * @return Metadata about the program execution, including the required
     *         {@link RuleStats#mutationCount}.
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
//BTM - PRE_FRED_3481       protected RuleStats executeMutation(final IStep step)
//BTM - PRE_FRED_3481            throws InterruptedException, ExecutionException {
//BTM - PRE_CLIENT_SERVICE  protected RuleStats executeMutation(final IStep step, IIndexManager indexManager, IConcurrencyManager concurrencyManager) throws InterruptedException, ExecutionException {

    protected RuleStats executeMutation
                            (final IStep step,
                             IIndexManager indexManager,
                             IConcurrencyManager concurrencyManager,
                             IBigdataDiscoveryManagement discoveryManager)
                        throws InterruptedException, ExecutionException
    {
        if (step == null)
            throw new IllegalArgumentException();

        if (!action.isMutation())
            throw new IllegalArgumentException();

        long tx = 0L;
        TransactionService txnService = null;
        try {
//BTM - PRE_CLIENT_SERVICE  if (indexManager instanceof IBigdataFederation) {
            if (discoveryManager != null) {

                /*
                 * Advance the read-consistent timestamp so that any writes from the
                 * previous rules or the last round are now visible.
                 *
                 * Note: We can only do this for the federation with its autoCommit
                 * semantics.
                 *
                 * Note: The Journal (LTS) must both read and write against the
                 * unisolated view for closure operations.
                 *
                 * @todo clone the joinNexusFactory 1st to avoid possible side
                 * effects?
                 */

                final long lastCommitTime = indexManager.getLastCommitTime();

                try {
                    /*
                     * A read-only tx reading from the lastCommitTime.
                     *
                     * Note: This provides a read-lock on the commit time from which
                     * the mutation task will read.
                     *
                     * @todo we could use the [tx] as the readTimestamp and we could
                     * use ITx.READ_COMMITTED rather that explicitly looking up the
                     * lastCommitTime.
                     */
//BTM - PRE_CLIENT_SERVICE  tx = ((IBigdataFederation) indexManager).getTransactionService().newTx(lastCommitTime);
                    txnService = discoveryManager.getTransactionService();
                    tx = txnService.newTx(lastCommitTime);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

                // the timestamp that we will read on for this step.
                joinNexusFactory.setReadTimestamp(TimestampUtility
                                    .asHistoricalRead(lastCommitTime));

//                final long lastCommitTime = indexManager.getLastCommitTime();
//
//                // the timestamp that we will read on for this step.
//                joinNexusFactory.setReadTimestamp(TimestampUtility
//                                    .asHistoricalRead(lastCommitTime));
//
//                try {
//
//                    /*
//                     * Allow the data services to release data for older views.
//                     *
//                     * FIXME This is being done in order to prevent the release of
//                     * data associated with the [readTimestamp] while the rule(s)
//                     * are reading on those views. In fact, the rules should be
//                     * using a read-historical transaction and releasing the
//                     * transaction when they are finished which would allow the
//                     * transaction manager to take responsibility for globally
//                     * determining the releaseTime for the federation. In the
//                     * absence of that facility, this has been hacked so that we can
//                     * do scale-out closure without having our views disrupted by
//                     * overflow allowing resources to be purge that are in use by
//                     * those views. Basically, we are declaring a read lock.
//                     *
//                     * The problem discussed here can be readily observed if you
//                     * attempt the closure of a large data set (U20 may do it, U50
//                     * will) or if you reduce the journal extent so that overflow is
//                     * triggered more frequently, in which case you may be able to
//                     * demonstrate the problem with a much smaller data set.
//                     *
//                     * Also note that this is globally advancing the release time.
//                     * This means that you can not compute closure for two different
//                     * RDF DBs within the same federation at the same time.
//                     *
//                     * Also see the code that resets the release time to 0L after
//                     * the ProgramTask is finished.
//                     */
//
//                    if (lastCommitTime != 0L) {
//
//                        /*
//                         * There is some committed state, so update the release time
//                         * such that the timestamp specified by [lastCommitTime]
//                         * does not get released.
//                         */
//
//                        final long releaseTime = lastCommitTime - 1;
//
//                        log.warn("readLock: releaseTime="+releaseTime+", step="+step.getName());
//
//                        ((IBigdataFederation) indexManager).getTransactionService()
//                                .setReleaseTime(releaseTime);
//
//                    }
//
//                } catch (IOException e) {
//
//                    throw new RuntimeException(e);
//
//                }

            }//endif(discoveryManager != null)

//BTM        final MutationTask mutationTask = new MutationTask(action, joinNexusFactory,
//BTM                step, indexManager, isDataService()?getDataService():null);

//BTM - PRE_FRED_3481 final MutationTask mutationTask = new MutationTask(action, joinNexusFactory,
//BTM - PRE_FRED_3481         step, indexManager,
//BTM - PRE_FRED_3481         getResourceManager(),
//BTM - PRE_FRED_3481         getConcurrencyManager(),
//BTM - PRE_FRED_3481         getIndexManager(),
//BTM - PRE_FRED_3481         getSession(),
//BTM - PRE_FRED_3481         getHostname(),
//BTM - PRE_FRED_3481         getServiceName());

//BTM - PRE_CLIENT_SERVICE            final MutationTask mutationTask = 
//BTM - PRE_CLIENT_SERVICE                  new MutationTask(action, joinNexusFactory, step, indexManager);

            final MutationTask mutationTask = 
                  new MutationTask(action, joinNexusFactory, step,
                                   indexManager, concurrencyManager,
                                   discoveryManager);

            if (log.isDebugEnabled())
                log.debug("begin: action=" + action + ", program=" + step.getName()
                        + ", task=" + mutationTask);

            /*
             * Submit task and await completion, returning the result.
             *
             * Note: The task is responsible for computing the aggregate mutation
             * count.
             */
//BTM - PRE_FRED_3481            return mutationTask.submit().get();
                return mutationTask.submit(concurrencyManager).get();
        } finally {
//BTM - PRE_CLIENT_SERVICE  if (tx != 0L) {
            if (txnService != null) {// => tx != 0L
                // terminate the read-only tx (releases the read-lock).
                try {
//BTM - PRE_CLIENT_SERVICE  ((IBigdataFederation) indexManager).getTransactionService().abort(tx);
                    txnService.abort(tx);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    /**
     * Computes the closure of a set of {@link IRule}s until the relation(s) on
     * which they are writing reach a "fixed point".
     * <p>
     * The general approach is a series of rounds in which each rule is applied
     * in turn (either sequentially or in parallel, depending on the program).
     * Solutions computed for each rule in each round written onto the relation
     * for the head of that rule. The process halts when no new solutions are
     * computed in a given round.
     * <p>
     * Note: When we are running the program on a {@link ConcurrencyManager},
     * each round of the closure is submitted as a single {@link AbstractTask}.
     * This allows us to do historical reads during the round and to update the
     * read-behind timestamp before each round. During the round, the program
     * will write on buffers that are flushed at the end of the round. Those
     * buffers will use unisolated writes onto the appropriate relations.
     *
     * <h2>mutation counts</h2>
     *
     * In order to detect the fixed point we MUST know whether or not any
     * mutations were made to the relation during the round. The design does NOT
     * rely on the relation count before and after the round since it would have
     * to use an _exact_ range count for the relation (otherwise it is possible
     * that a deleted tuple would be overwritten by a computed entailment but
     * that the count would not change). However, the exact range count is
     * relatively expensive which is why the design insists on getting back the
     * #of elements actually written on the index from each rule in each round.
     * If no rules in a given round caused an element to be written, then we are
     * at the fixed point.
     * <p>
     * Note: This assumes that you are following the {@link IMutableRelation}
     * contract -- you MUST NOT overwrite tuples with the same key and value, or
     * at least you must not report such "do nothing" overwrites in the mutation
     * count!!!
     *
     * @param action
     *            The action (must be a mutation operation).
     * @param program
     *            The program to be executed.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
//BTM - PRE_FRED_3481       protected RuleStats executeClosure(final IProgram program)
//BTM - PRE_FRED_3481            throws InterruptedException, ExecutionException {
//BTM - PRE_CLIENT_SERVICE  protected RuleStats executeClosure(final IProgram program, IIndexManager indexManager, IConcurrencyManager concurrencyManager) throws InterruptedException, ExecutionException {

    protected RuleStats executeClosure
                           (final IProgram program,
                            IIndexManager indexManager,
                            IConcurrencyManager concurrencyManager,
                            IBigdataDiscoveryManagement discoveryManager)
              throws InterruptedException, ExecutionException
    {
        if (program == null)
            throw new IllegalArgumentException();

        if(!program.isClosure())
            throw new IllegalArgumentException();

        final long begin = System.currentTimeMillis();

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE  final RuleStats totals =
//BTM - PRE_CLIENT_SERVICE        joinNexusFactory.newInstance(indexManager).getRuleStatisticsFactory().newInstance(program);
//BTM - PRE_CLIENT_SERVICE
        final RuleStats totals = 
              joinNexusFactory.newInstance
                                  (indexManager,
                                   concurrencyManager,
                                   discoveryManager).getRuleStatisticsFactory()
                                                    .newInstance(program);
//BTM - PRE_CLIENT_SERVICE - END

        int round = 1;

        long mutationCount = 0L;
        while (true) {

            // mutationCount before this round.
            final long mutationCount0 = totals.mutationCount.get();

            if (log.isDebugEnabled())
                log.debug("round=" + round + ", mutationCount(before)="
                        + mutationCount0);

//            if (round > 1) {
//
//                /*
//                 * Advance the read-consistent timestamp so that any writes from
//                 * the previous rules or the last round are now visible.
//                 */
//
//                joinNexusFactory.setReadTimestamp(TimestampUtility
//                        .asHistoricalRead(indexManager.getLastCommitTime()));
//
//            }

            // execute the program.
//BTM - PRE_FRED_3481       final RuleStats tmp = executeMutation(program);
//BTM - PRE_CLIENT_SERVICE  final RuleStats tmp = executeMutation(program, indexManager, concurrencyManager);
            final RuleStats tmp = 
                      executeMutation(program,
                                      indexManager,
                                      concurrencyManager,
                                      discoveryManager);
            /*
             * This is the #of mutations from executing this round.
             *
             * Note: each round has its own mutation buffer so this is just the
             * #of mutations in the round. This is because executeMutation()
             * builds a new execution context for each round.
             */
            final long mutationDelta = tmp.mutationCount.get();

            // Total mutation count so far.
            final long mutationCount1 = mutationCount = mutationCount0
                    + tmp.mutationCount.get();

            // set the round identifier.
            tmp.closureRound = round;

            // Aggregate the rule statistics, but not mutationCount.
            totals.add(tmp);

            if (log.isDebugEnabled()) {

                log.debug("round# " + round + ", mutationCount(before="
                        + mutationCount0 + ", after=" + mutationCount1
                        + ", delta=" + mutationDelta + "):" + totals);

            }

            if (mutationDelta == 0L)
                break;

            round++;

        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (!totals.mutationCount.compareAndSet(0L, mutationCount)) {

            throw new AssertionError("mutationCount=" + totals.mutationCount);

        }

        if (log.isInfoEnabled()) {

            log.info("\nComputed fixed point: program=" + program.getName()
                    + ", rounds=" + round + ", elapsed=" + elapsed + "ms");

        }

        return totals;

    }

    /**
     * Execute an {@link IProgram} containing one or more sub-{@link IProgram}
     * that are closure operations. The top-level program must not be a closure
     * operation. All steps above the closure operations will be run in a
     * sequence. The closure operations themselves will be executed using
     * {@link #executeClosure(IProgram)}.
     * <p>
     * Note: Any program that embeds a closure operation must be sequential
     * (this is enforced by the Program class).
     *
     * Note: Programs that use closure operations are constrained to either (a)
     * a fix point of a (normally parallel) program consisting solely of
     * {@link IRule}s; or (b) a sequential program containing some steps that
     * are the fix point of a (normally parallel) program consisting solely of
     * {@link IRule}s.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     *
     * @todo this will not correctly handle programs use closure in a
     *       sub-sub-program.
     *
     * @throws IllegalArgumentException
     *             if <i>program</i> is <code>null</code>
     * @throws IllegalArgumentException
     *             if <i>program</i> is <em>itself</em> a closure operation.
     * @throws IllegalStateException
     *             unless the {@link ActionEnum} is a mutation operation.
     */
//BTM - PRE_FRED_3481       protected RuleStats executeProgramWithEmbeddedClosure(final IProgram program) throws InterruptedException, ExecutionException {
//BTM - PRE_CLIENT_SERVICE  protected RuleStats executeProgramWithEmbeddedClosure(final IProgram program, IIndexManager indexManager, IConcurrencyManager concurrencyManager) throws InterruptedException, ExecutionException {

    protected RuleStats executeProgramWithEmbeddedClosure
                            (final IProgram program,
                             IIndexManager indexManager,
                             IConcurrencyManager concurrencyManager,
                             IBigdataDiscoveryManagement discoveryManager)
              throws InterruptedException, ExecutionException
    {
        if (program == null)
            throw new IllegalArgumentException();

        if (program.isClosure())
            throw new IllegalArgumentException();

        if(!action.isMutation()) {
            throw new IllegalStateException();
        }

        if (log.isInfoEnabled())
            log.info("program embeds closure operations");

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE  final RuleStats totals =
//BTM - PRE_CLIENT_SERVICE        joinNexusFactory.newInstance(indexManager).getRuleStatisticsFactory().newInstance(program);
//BTM - PRE_CLIENT_SERVICE
        final RuleStats totals =
              joinNexusFactory.newInstance
                  (indexManager,
                   concurrencyManager,
                   discoveryManager).getRuleStatisticsFactory()
                                    .newInstance(program);
//BTM - PRE_CLIENT_SERVICE - END

        final Iterator<? extends IStep> itr = (program).steps();

        long mutationCount = 0L;
        while(itr.hasNext()) {

            final IStep tmpStep = itr.next();

            final RuleStats stats;

            if (!tmpStep.isRule() && ((IProgram) tmpStep).isClosure()) {

                // A closure step.
//BTM - PRE_FRED_3481       stats = executeClosure((IProgram) step);
//BTM - PRE_CLIENT_SERVICE  stats = executeClosure((IProgram) tmpStep, indexManager, concurrencyManager);
                stats = executeClosure( (IProgram) tmpStep,
                                        indexManager,
                                        concurrencyManager,
                                        discoveryManager );
            } else {

                // A non-closure step.
//BTM - PRE_FRED_3481       stats = executeMutation(step);
//BTM - PRE_CLIENT_SERVICE  stats = executeMutation(tmpStep, indexManager, concurrencyManager);
                stats = executeMutation(tmpStep,
                                        indexManager,
                                        concurrencyManager,
                                        discoveryManager);
            }

            totals.add(stats);

            /*
             * Note: both a executeClosure() and executeMutation() will run with
             * their own buffers so flush() reporting is not carried forward
             * beyond those methods. Hence we have to aggregate the
             * mutationCount ourselves for each step that we run.
             */
            mutationCount += stats.mutationCount.get();

        }

        // transfer the final mutation count onto the total.
        if (!totals.mutationCount.compareAndSet(0L, mutationCount)) {

            throw new AssertionError("mutationCount=" + totals.mutationCount);

        }

        return totals;

    }

}
