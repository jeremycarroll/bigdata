/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 21, 2010
 */

package com.bigdata.bop.engine;

import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.view.FusedView;
import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.QueryHints;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.resources.IndexManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.IHaltable;

/**
 * A class managing execution of concurrent queries against a local
 * {@link IIndexManager}.
 * <p>
 * <h2>Design notes</h2>
 * <p>
 * Much of the complexity of the current approach owes itself to having to run a
 * separate task for each join for each shard in order to have the appropriate
 * lock when running against the unisolated shard view. This also means that the
 * join task is running inside of the concurrency manager and hence has the
 * local view of the shard.
 * <p>
 * The main, and perhaps the only, reason why we run unisolated rules is during
 * closure, when we query against the unisolated indices and then write the
 * entailments back on the unisolated indices.
 * <p>
 * Supporting closure has always been complicated. This complexity is mostly
 * handled by ProgramTask#executeMutation() and
 * AbstractTripleStore#newJoinNexusFactory() which play games with the
 * timestamps used to read and write on the database, with commit points
 * designed to create visibility for tuples written by a mutation rule, and with
 * the automated advance of the read timestamp for the query in each closure
 * pass in order to make newly committed tuples visible to subsequent rounds of
 * closure. For scale-out, we do shard-wise auto commits so we always have a
 * commit point which makes each write visible and the read timestamp is
 * actually a read-only transaction which prevents the historical data we need
 * during a closure round from being released as we are driving updates onto the
 * federation. For the RWStore, we are having a similar problem (in the HA
 * branch since that is where we are working on the RWStore) where historically
 * allocated records were being released as writes drove updates on the indices.
 * Again, we "solved" the problem for the RWStore using a commit point followed
 * by a read-only transaction reading on that commit point to hold onto the view
 * on which the next closure round needs to read (this uncovered a problem with
 * the RWStore and transaction service interaction which Martyn is currently
 * working to resolve through a combination of shadow allocators and deferred
 * deletes which are processed once the release time is advanced by the
 * transaction service).
 * <p>
 * The WORM does not have some of these problems with closure because we never
 * delete history, so we do not need to create a commit point and a read-behind
 * transaction. However, the WORM would have problems with concurrent access to
 * the unisolated indices except that we hack that problem through the
 * transparent use of the UnisolatedReadWriteIndex, which allows multiple
 * threads to access the same unisolated index view using a read/write lock
 * pattern (concurrent readers are allowed, but there is only one writer and it
 * has exclusive access when it is running). This works out because we never run
 * closure operations against the WORM through the concurrency manager. If we
 * did, we would have to create a commit point after each mutation and use a
 * read-behind transaction to prevent concurrent access to the unisolated index.
 * <p>
 * The main advantage that I can see of the current complexity is that it allows
 * us to do load+closure as a single operation on the WORM, resulting in a
 * single commit point. This makes that operation ACID without having to use
 * full read/write transactions. This is how we gain the ACID contract for the
 * standalone Journal in the SAIL for the WORM. Of course, the SAIL does not
 * have that contract for the RWStore because we have to do the commit and
 * read-behind transaction in order to have visibility and avoid concurrent
 * access to the unisolated index (by reading behind on the last commit point).
 * <p>
 * I think that the reality is even one step more complicated. When doing truth
 * maintenance (incremental closure), we bring the temporary graph to a fixed
 * point (the rules write on the temp store) and then apply the delta in a
 * single write to the database. That suggests that incremental truth
 * maintenance would continue to be ACID, but that database-at-once-closure
 * would be round-wise ACID.
 * <p>
 * So, I would like to suggest that we break ACID for database-at-once-closure
 * and always follow the pattern of (1) do a commit before each round of
 * closure; and (2) create a read-behind transaction to prevent the release of
 * that commit point as we drive writes onto the indices. If we follow this
 * pattern then we can write on the unisolated indices without conflict and read
 * on the historical views without conflict. Since there will be a commit point
 * before each mutation rule runs (which corresponds to a closure round),
 * database-at-once-closure will be atomic within a round, but will not be a
 * single atomic operation. Per above, I think that we would retain the ACID
 * property for incremental truth maintenance against a WORM or RW mode Journal.
 * 
 * <p>
 * ----
 * </p>
 * 
 * The advantage of this proposal (commit before each mutation rule and run
 * query against a read-behind transaction) is that this could enormously
 * simplify how we execute joins.
 * <p>
 * Right now, we use a factory pattern to create a join task on each node for
 * each shard for which that node receives binding sets for a query. The main
 * reason for doing this is to gain the appropriate lock for the unisolated
 * index. If we never run a query against the unisolated index then we can go
 * around the concurrency manager and run a single "query manager" task for all
 * joins for all shards for all queries. This has some great benefits which I
 * will go into below.
 * <p>
 * That "query manager" task would be responsible for accepting buffers
 * containing elements or binding sets from other nodes and scheduling
 * consumption of those data based on various criteria (order of arrival,
 * priority, buffer resource requirements, timeout, etc.). This manager task
 * could use a fork join pool to execute light weight operations (NIO,
 * formulation of access paths from binding sets, mapping of binding sets onto
 * shards, joining a chunk already read from an access path against a binding
 * set, etc). Operations which touch the disk need to run in their own thread
 * (until we get Java 7 async file IO, which is already available in a preview
 * library). We could handle that by queuing those operations against a fixed
 * size thread pool for reads.
 * <p>
 * This is a radical change in how we handle distributed query execution, but I
 * think that it could have a huge payoff by reducing the complexity of the join
 * logic, making it significantly easier to execute different kinds of join
 * operations, reducing the overhead for acquiring locks for the unisolated
 * index views, reducing the #of threads consumed by joins (from one per shard
 * per join per query to a fixed pool of N threads for reads), etc. It would
 * centralize the management of resources on each node and make it possible for
 * us to handle things like join termination by simply purging data from the
 * query manager task for the terminated join.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Expander patterns will continue to exist until we handle the standalone
 *       backchainers in a different manner for scale-out so add support for
 *       those for now.
 * 
 * @todo There is a going to be a relationship to recycling of intermediates
 *       (for individual {@link BOp}s or {@link BOp} tree fragments) and a
 *       distributed query cache which handles invalidation (for updates) and
 *       {@link BOp} aware reuse of result sets available in the cache. This
 *       sort of thing will have to be coordinated among the cache nodes.
 */
public class QueryEngine implements IQueryPeer, IQueryClient {
    
    private final static transient Logger log = Logger
            .getLogger(QueryEngine.class);

    /**
     * Error message used if a query is not running.
     */
    protected static final transient String ERR_QUERY_NOT_RUNNING = "Query is not running:";

    /**
     * Annotations understood by the {@link QueryEngine}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public interface Annotations extends PipelineOp.Annotations {

		/**
		 * Annotation may be used to impose a specific {@link UUID} for a query.
		 * This may be used by an external process such that it can then use
		 * {@link QueryEngine#getRunningQuery(UUID)} to gain access to the
		 * running query instance. It is an error if there is a query already
		 * running with the same {@link UUID}.
		 */
		String QUERY_ID = (QueryEngine.class.getName() + ".runningQueryClass")
				.intern();
    	
        /**
         * The name of the {@link IRunningQuery} implementation class which will
         * be used to evaluate a query marked by this annotation (optional). The
         * specified class MUST implement {@link IRunningQuery} and MUST have a
         * constructor with the following signature:
         * 
         * <pre>
         * public MyRunningQuery(QueryEngine queryEngine, UUID queryId,
         *             boolean controller, IQueryClient clientProxy,
         *             PipelineOp query)
         * </pre>
         * 
         * Note that classes derived from {@link QueryEngine} may override
         * {@link QueryEngine#newRunningQuery(QueryEngine, UUID, boolean, IQueryClient, PipelineOp)}
         * in which case they might not support this option.
         */
        String RUNNING_QUERY_CLASS = (QueryEngine.class.getName()
                + ".runningQueryClass").intern();

//        String DEFAULT_RUNNING_QUERY_CLASS = StandaloneChainedRunningQuery.class.getName();
        String DEFAULT_RUNNING_QUERY_CLASS = ChunkedRunningQuery.class.getName();

    }

	/**
	 * Statistics collected for queries.
	 * 
	 * @author thompsonbry
	 */
    protected static class Counters {

		/**
		 * The #of queries which have been executed (set on completion).
		 */
		final CAT startCount = new CAT();
		
		/**
		 * The #of queries which have been executed (set on completion).
		 */
		final CAT doneCount = new CAT();

		/**
		 * The #of instances of the query which terminated abnormally.
		 */
		final CAT errorCount = new CAT();

		/**
		 * The total elapsed time (millis) for evaluation queries. This is the
		 * wall clock time per query. The aggregated wall clock time per query
		 * will sum to greater than the elapsed wall clock time in any interval
		 * where there is more than one query running concurrently.
		 */
		final CAT elapsedMillis = new CAT();

		public CounterSet getCounters() {

			final CounterSet root = new CounterSet();

			// #of queries started on this server.
			root.addCounter("startCount", new Instrument<Long>() {
				public void sample() {
					setValue(startCount.get());
				}
			});

			// #of queries retired on this server.
			root.addCounter("doneCount", new Instrument<Long>() {
				public void sample() {
					setValue(doneCount.get());
				}
			});

			// #of queries with abnormal termination on this server.
			root.addCounter("errorCount", new Instrument<Long>() {
				public void sample() {
					setValue(errorCount.get());
				}
			});

			// #of queries retired per second on this server.
			root.addCounter("queriesPerSecond", new Instrument<Double>() {
				public void sample() {
					final long ms = elapsedMillis.get();
					final long n = doneCount.get();
					// compute throughput, normalized to q/s := (q*1000)/ms.
					final double d = ms == 0 ? 0d : ((1000d * n) / ms);
					setValue(d);
				}
			});

			return root;

		}
		
    }

    /**
     * Return a {@link CounterSet} which reports various statistics for the
     * {@link QueryEngine}.
     */
    public CounterSet getCounters() {

		final CounterSet root = new CounterSet();

		// global counters.
        root.attach(counters.getCounters());
		
        // counters per tagged query group.
		{

			final CounterSet groups = root.makePath("groups");

			final Iterator<Map.Entry<String, Counters>> itr = groupCounters
					.entrySet().iterator();

			while (itr.hasNext()) {

				final Map.Entry<String, Counters> e = itr.next();

				final String tag = e.getKey();

				final Counters counters = e.getValue();

				// Note: path component may not be empty!
				groups.makePath(tag == null | tag.length() == 0 ? "None" : tag)
						.attach(counters.getCounters());

			}

		}

		return root;

    }
    
    /**
     * Counters at the global level.
     */
	final protected Counters counters = newCounters();

	/**
	 * Statistics for queries which are "tagged" so we can recognize their
	 * instances as members of some group.
	 */
	final protected ConcurrentHashMap<String/* groupId */, Counters> groupCounters = new ConcurrentHashMap<String, Counters>();

	/**
	 * Factory for {@link Counters} instances associated with a query group. A
	 * query is marked as a member of a group using {@link QueryHints#TAG}. This
	 * is typically used to mark queries which are instances of the same query
	 * template.
	 * 
	 * @param tag
	 *            The tag identifying a query group.
	 * 
	 * @return The {@link Counters} for that query group.
	 * 
	 * @throws IllegalArgumentException
	 *             if the argument is <code>null</code>.
	 */
	protected Counters getCounters(final String tag) {

		if(tag == null)
			throw new IllegalArgumentException();
		
		Counters c = groupCounters.get(tag);

		if (c == null) {

			c = new Counters();

			final Counters tmp = groupCounters.putIfAbsent(tag, c);

			if (tmp != null) {

				// someone else won the data race.
				c = tmp;

			}

		}

		return c;

	}

	/**
	 * Extension hook for new {@link Counters} instances.
	 */
	protected Counters newCounters() {
		
		return new Counters();
		
	}
	
    /**
     * Access to the indices.
     * <p>
     * Note: You MUST NOT use unisolated indices without obtaining the necessary
     * locks. The {@link QueryEngine} is intended to run only against committed
     * index views for which no locks are required.
     */
    private final IIndexManager localIndexManager;

//    /**
//     * A pool used to service IO requests (reads on access paths).
//     * <p>
//     * Note: An IO thread pool at this level must attach threads to operations
//     * (access path reads) rather than to individual IO requests. In order to do
//     * this at the level of individual IOs the pool would have to be integrated
//     * into a lower layer, probably wrapping {@link FileChannelUtility}.
//     */
//    private final Executor iopool;

//    /**
//     * A pool for executing fork/join tasks servicing light weight tasks which
//     * DO NOT block on IO. Examples of such tasks abound, including: NIO for
//     * sending/receiving direct {@link ByteBuffer}s containing binding sets,
//     * elements, solutions, etc; formulation of access paths from binding sets;
//     * mapping of binding sets onto shards; joining a chunk already read from an
//     * access path against a binding set; etc. What all of these tasks have in
//     * common is that they DO NOT touch the disk. Until we get Java7 and async
//     * I/O, operations which touch the disk CAN NOT be combined with the fork /
//     * join model since they will trap the thread in which they are running
//     * (this is not true for {@link Lock}s).
//     * <p>
//     * Note: In order to enable the {@link ForkJoinPool} using Java6, you MUST
//     * run java with <code>-Xbootclasspath/p:jsr166.jar</code>, where you
//     * specify the fully qualified path of the jsr166.jar file.
//     */
//    private final ForkJoinPool fjpool;

    /**
     * The {@link UUID} of the service in which this {@link QueryEngine} is
     * running.
     * 
     * @return The {@link UUID} of the service in which this {@link QueryEngine}
     *         is running -or- <code>null</code> if the {@link QueryEngine} is
     *         not running against an {@link IBigdataFederation}.
     */
    public UUID getServiceUUID() {

        return null;
        
    }

    /**
     * The {@link IBigdataFederation} iff running in scale-out.
     * <p>
     * Note: The {@link IBigdataFederation} is required in scale-out in order to
     * perform shard locator scans when mapping binding sets across the next
     * join in a query plan.
     */
    public IBigdataFederation<?> getFederation() {
        
        return null;
        
    }

    /**
     * The <em>local</em> index manager, which provides direct access to local
     * {@link BTree} and {@link IndexSegment} objects. In scale-out, this is the
     * {@link IndexManager} inside the {@link IDataService} and provides direct
     * access to {@link FusedView}s (aka shards).
     * <p>
     * Note: You MUST NOT use unisolated indices without obtaining the necessary
     * locks. The {@link QueryEngine} is intended to run only against committed
     * index views for which no locks are required.
     */
    public IIndexManager getIndexManager() {
        
        return localIndexManager;
        
    }
    
    /**
     * The RMI proxy for this {@link QueryEngine} when used as a query controller.
     * The default implementation returns <i>this</i>.
     */
    public IQueryClient getProxy() {

    	return this;
    	
    }
    
    /**
     * Return <code>true</code> iff running against an
     * {@link IBigdataFederation}.
     */
    public boolean isScaleOut() {

        return false;

    }

    /**
     * Lock used to guard register / halt of a query.
     */
    private final ReentrantLock lock = new ReentrantLock();
    
    /**
     * Signaled when no queries are running.
     */
    private final Condition nothingRunning = lock.newCondition();
    
    /**
     * The currently executing queries.
     */
    final private ConcurrentHashMap<UUID/* queryId */, AbstractRunningQuery> runningQueries = new ConcurrentHashMap<UUID, AbstractRunningQuery>();

	/**
	 * LRU cache used to handle problems with asynchronous termination of
	 * running queries.
	 * <p>
	 * Note: Holding onto the query references here might pin memory retained by
	 * those queries. However, all we really need is the Haltable (Future) of
	 * that query in this map.
	 * 
	 * @todo This should not be much of a hot spot even though it is not thread
	 *       safe but the synchronized() call could force cache stalls anyway. A
	 *       concurrent hash map with an approximate LRU access policy might be
	 *       a better choice.
	 * 
	 * @todo The maximum cache capacity here is a SWAG. It should be large
	 *       enough that we can not have a false cache miss on a system which is
	 *       heavily loaded by a bunch of light queries.
	 */
	private LinkedHashMap<UUID, IHaltable<Void>> doneQueries = new LinkedHashMap<UUID,IHaltable<Void>>(
			16/* initialCapacity */, .75f/* loadFactor */, true/* accessOrder */) {

		private static final long serialVersionUID = 1L;

		@Override
		protected boolean removeEldestEntry(Map.Entry<UUID, IHaltable<Void>> eldest) {

			return size() > 100/* maximumCacheCapacity */;

		}
	};

	/**
	 * A queue of {@link ChunkedRunningQuery}s having binding set chunks available for
	 * consumption.
	 * 
	 * @todo Handle priority for selective queries based on the time remaining
	 *       until the timeout.
	 *       <p>
	 *       Handle priority for unselective queries based on the order in which
	 *       they are submitted?
	 *       <p>
	 *       Be careful when testing out a {@link PriorityBlockingQueue} here.
	 *       First, that collection is intrinsically bounded (it is backed by an
	 *       array) so it will BLOCK under heavy load and could be expected to
	 *       have some resize costs if the queue size becomes too large. Second,
	 *       either {@link ChunkedRunningQuery} needs to implement an appropriate
	 *       {@link Comparator} or we need to pass one into the constructor for
	 *       the queue.
	 */
    final private BlockingQueue<AbstractRunningQuery> priorityQueue = new LinkedBlockingQueue<AbstractRunningQuery>();
//    final private BlockingQueue<RunningQuery> priorityQueue = new PriorityBlockingQueue<RunningQuery>(
//            );

    /**
     * 
     * @param indexManager
     *            The <em>local</em> index manager.
     */
    public QueryEngine(final IIndexManager indexManager) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        this.localIndexManager = indexManager;

//        this.iopool = new LatchedExecutor(indexManager.getExecutorService(),
//                nThreads);
//        this.iopool = Executors.newFixedThreadPool(nThreads,
//                new DaemonThreadFactory(getClass().getName()));
        
//        this.fjpool = new ForkJoinPool();

    }

    /**
     * Initialize the {@link QueryEngine}. It will accept binding set chunks and
     * run them against running queries until it is shutdown.
     */
    public void init() {

        final FutureTask<Void> ft = new FutureTaskMon<Void>(new QueryEngineTask(
                priorityQueue), (Void) null);

        if (engineFuture.compareAndSet(null/* expect */, ft)) {
        
            engineService.set(Executors
                    .newSingleThreadExecutor(new DaemonThreadFactory(
                            QueryEngine.class + ".engineService")));

            engineService.get().execute(ft);

        } else {
            
            throw new IllegalStateException("Already running");
            
        }

    }

    /**
     * {@link QueryEngine}s are used with a singleton pattern managed by the
     * {@link QueryEngineFactory}. They are torn down automatically once they
     * are no longer reachable. This behavior depends on not having any hard
     * references back to the {@link QueryEngine}.
     */
    @Override
    protected void finalize() throws Throwable {
        
        shutdownNow();
        
        super.finalize();
        
    }
    
    /**
     * The service on which we run the query engine.  This is started by {@link #init()}.
     */
    private final AtomicReference<ExecutorService> engineService = new AtomicReference<ExecutorService>();

    /**
     * The {@link Future} for the query engine.  This is set by {@link #init()}.
     */
    private final AtomicReference<FutureTask<Void>> engineFuture = new AtomicReference<FutureTask<Void>>();
    
    /**
     * Volatile flag is set for normal termination.  When set, no new queries
     * will be accepted but existing queries will run to completion.
     */
    private volatile boolean shutdown = false;

    /**
     * Return if the query engine is running.
     * 
     * @throws IllegalStateException
     *             if the query engine is shutting down.
     */
    protected void assertRunning() {

        if (engineFuture.get() == null)
            throw new IllegalStateException("Not initialized.");
        
        if (shutdown)
            throw new IllegalStateException("Shutting down.");

    }

    protected boolean isRunning() {

    	return engineFuture.get() != null && !shutdown;

    }
    
    protected void execute(final Runnable r) {
        
        localIndexManager.getExecutorService().execute(r);
        
    }
    
    /**
     * Runnable submits chunks available for evaluation against running queries.
     * <p>
     * Note: This is a static inner class in order to avoid a hard reference 
     * back to the outer {@link QueryEngine} object.  This makes it possible
     * for the JVM to finalize the {@link QueryEngine} if the application no
     * longer holds a hard reference to it.  The {@link QueryEngine} is then
     * automatically closed from within its finalizer method.
     */
    static private class QueryEngineTask implements Runnable {
        
        final private BlockingQueue<AbstractRunningQuery> queue;

        public QueryEngineTask(final BlockingQueue<AbstractRunningQuery> queue) {

            if (queue == null)
                throw new IllegalArgumentException();
            
            this.queue = queue;
            
        }
        
        public void run() {
            if(log.isInfoEnabled())
                log.info("Running: " + this);
            while (true) {
                try {
                    final AbstractRunningQuery q = queue.take();
                    if (!q.isDone())
                        q.consumeChunk();
                } catch (InterruptedException e) {
                    /*
                     * Note: Uncomment the stack trace here if you want to find
                     * where the query was interrupted.
                     * 
                     * Note: If you want to find out who interrupted the query,
                     * then you can instrument BlockingBuffer#close() in
                     * PipelineOp#newBuffer(stats).
                     */
                    if (log.isInfoEnabled())
                        log.info("Interrupted."
//                            ,e
                            );
                    return;
                } catch (Throwable t) {
                    // log and continue
                    log.error(t, t);
                    continue;
                }
            }
        }
    } // QueryEngineTask

	/**
	 * Add a chunk of intermediate results for consumption by some query. The
	 * chunk will be attached to the query and the query will be scheduled for
	 * execution.
	 * 
	 * @param msg
	 *            A chunk of intermediate results.
	 * 
	 * @return <code>true</code> if the chunk was accepted. This will return
	 *         <code>false</code> if the query is done (including cancelled) or
	 *         the query engine is shutdown.
	 * 
	 * @throws IllegalArgumentException
	 *             if the chunk is <code>null</code>.
	 * @throws IllegalStateException
	 *             if the chunk is not materialized.
	 */
    protected boolean acceptChunk(final IChunkMessage<IBindingSet> msg) {
        
        if (msg == null)
            throw new IllegalArgumentException();

        if (!msg.isMaterialized())
            throw new IllegalStateException();

        final AbstractRunningQuery q = getRunningQuery(msg.getQueryId());
        
        if(q == null) {
			/*
			 * The query is not registered on this node.
			 */
            throw new IllegalStateException();
        }
        
		// add chunk to the query's input queue on this node.
		if (!q.acceptChunk(msg)) {
			// query is no longer running.
			return false;
			
		}

		if(!isRunning()) {
			// query engine is no longer running.
			return false;
			
		}

		// add query to the engine's task queue.
		priorityQueue.add(q);

		return true;

    }

    /**
     * Shutdown the {@link QueryEngine} (blocking). The {@link QueryEngine} will
     * not accept new queries, but existing queries will run to completion.
     */
    public void shutdown() {

        // normal termination.
        shutdown = true;

		lock.lock();
		try {
			while (!runningQueries.isEmpty()) {
				try {
					nothingRunning.await();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		} finally {
			lock.unlock();
		}
        
        // hook for subclasses.
        didShutdown();
        
        // stop the query engine.
        final Future<?> f = engineFuture.get();
        if (f != null) {
            f.cancel(true/* mayInterruptIfRunning */);
        }

        // stop the service on which we ran the query engine.
        final ExecutorService s = engineService.get();
        if (s != null) {
            s.shutdownNow();
        }
        
        // clear references.
        engineFuture.set(null);
        engineService.set(null);
        
    }

    /**
     * Hook is notified by {@link #shutdown()} when all running queries have
     * terminated.
     */
    protected void didShutdown() {
        
    }
    
    /**
     * Do not accept new queries and halt any running binding set chunk tasks.
     */
    public void shutdownNow() {
        
        shutdown = true;
        
        // stop the query engine.
        final Future<?> f = engineFuture.get();
        if (f != null)
            f.cancel(true/* mayInterruptIfRunning */);

        // stop the service on which we ran the query engine.
        final ExecutorService s = engineService.get();
        if (s != null) {
            s.shutdownNow();
        }
        
        // halt any running queries.
        for(AbstractRunningQuery q : runningQueries.values()) {
            
            q.cancel(true/*mayInterruptIfRunning*/);
            
        }

        // clear references.
        engineFuture.set(null);
        engineService.set(null);

    }

    /*
     * IQueryPeer
     */
    
    public void declareQuery(final IQueryDecl queryDecl) {
        
        throw new UnsupportedOperationException();
        
    }
    
    public void bufferReady(final IChunkMessage<IBindingSet> msg) {

        throw new UnsupportedOperationException();

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * The default implementation is a NOP.
     */
    public void cancelQuery(final UUID queryId, final Throwable cause) {
        // NOP
    }

    /*
     * IQueryClient
     */

    public PipelineOp getQuery(final UUID queryId) {
        
        final AbstractRunningQuery q = getRunningQuery(queryId);
        
        if (q == null)
            throw new IllegalArgumentException();
        
        return q.getQuery();
        
    }

    public void startOp(final StartOpMessage msg) throws RemoteException {
        
        final AbstractRunningQuery q = getRunningQuery(msg.queryId);
        
        if (q != null) {
        
            q.startOp(msg);
            
        }

    }

    public void haltOp(final HaltOpMessage msg) throws RemoteException {
        
        final AbstractRunningQuery q = getRunningQuery(msg.queryId);
        
        if (q != null) {
            
            q.haltOp(msg);
            
        }
        
    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single, empty
     * {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    private ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });

    }

    /**
     * Evaluate a query. This node will serve as the controller for the query.
     * 
     * @param query
     *            The query to evaluate.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     */
    public AbstractRunningQuery eval(final BOp op) throws Exception {
        
        return eval(op, new ListBindingSet());
        
    }

    /**
     * Evaluate a query. This node will serve as the controller for the query.
     * 
     * @param query
     *            The query to evaluate.
     * @param bset
     *            The initial binding set to present.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     */
    public AbstractRunningQuery eval(final BOp op, final IBindingSet bset)
            throws Exception {
    	
    	// Use a random UUID unless the UUID was specified on the query.
    	final UUID queryId = op.getProperty(QueryEngine.Annotations.QUERY_ID,
				UUID.randomUUID());

        return eval(queryId, op, newBindingSetIterator(bset));

    }

    /**
     * Evaluate a query. This node will serve as the controller for the query.
     * 
     * @param queryId
     *            The unique identifier for the query.
     * @param query
     *            The query to evaluate.
     * @param bsets
     *            The binding sets to be consumed by the query.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     */
    public AbstractRunningQuery eval(final UUID queryId, final BOp op,
            final IAsynchronousIterator<IBindingSet[]> bsets) throws Exception {

        final BOp startOp = BOpUtility.getPipelineStart(op);

        final int startId = startOp.getId();

        return eval(queryId, (PipelineOp) op,
                new LocalChunkMessage<IBindingSet>(this/* queryEngine */,
                        queryId, startId, -1 /* partitionId */, bsets));

    }

    /**
     * Evaluate a query. This node will serve as the controller for the query.
     * The {@link IBindingSet}s made available by the {@link IChunkMessage} will
     * be pushed into the query.
     * 
     * @param queryId
     *            The unique identifier for the query.
     * @param query
     *            The query to evaluate.
     * @param msg
     *            A message providing access to the initial {@link IBindingSet
     *            binding set(s)} used to begin query evaluation.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     */
    public AbstractRunningQuery eval(final UUID queryId,
            final PipelineOp query,
            final IChunkMessage<IBindingSet> msg) throws Exception {

        if (queryId == null)
            throw new IllegalArgumentException();

        if (query == null)
            throw new IllegalArgumentException();

        if (msg == null)
            throw new IllegalArgumentException();

        if (!queryId.equals(msg.getQueryId()))
            throw new IllegalArgumentException();

        final AbstractRunningQuery runningQuery = newRunningQuery(
                /* this, */queryId, true/* controller */,
                getProxy()/* queryController */, query);

        final long timeout = query.getProperty(BOp.Annotations.TIMEOUT,
                BOp.Annotations.DEFAULT_TIMEOUT);

        if (timeout < 0)
            throw new IllegalArgumentException(BOp.Annotations.TIMEOUT);

        if (timeout != Long.MAX_VALUE) {

            // Compute the deadline (may overflow if timeout is very large).
            final long deadline = System.currentTimeMillis() + timeout;

            if (deadline > 0) {
                /*
                 * Impose a deadline on the query.
                 */
                runningQuery.setDeadline(deadline);

            }

        }

        // Note: ChunkTask verifies this.
//        /*
//         * Verify that all bops from the identified bop to the root have an
//         * assigned bop. This is required in order for us to be able to target
//         * messages to those operators.
//         */
//        BOpUtility.verifyPipline(msg.getBOpId(), query);

        // verify query engine is running.
        assertRunning();

		// add to running query table.
		if (putIfAbsent(queryId, runningQuery) != runningQuery) {

			/*
			 * UUIDs should not collide when assigned randomly. However, the
			 * UUID may be imposed by an exterior process, such as a SPARQL end
			 * point, so it can access metadata about the running query even
			 * when it is not a direct client of the QueryEngine. This provides
			 * a safety check against UUID collisions which might be non-random.
			 */
			throw new RuntimeException("Query exists with that UUID: uuid="
					+ runningQuery.getQueryId());

		}

		final String tag = query.getProperty(QueryHints.TAG,
				QueryHints.DEFAULT_TAG);

		final Counters c = tag == null ? null : getCounters(tag);

		// track #of started queries.
		counters.startCount.increment();

		if (c != null)
			c.startCount.increment();

        // notify query start
        runningQuery.startQuery(msg);
        
        // tell query to consume the initial chunk.
        acceptChunk(msg);

        return runningQuery;

    }

    /*
     * Management of running queries.
     */

    /**
     * Places the {@link AbstractRunningQuery} object into the internal map.
     * 
     * @param queryId
     *            The query identifier.
     * @param runningQuery
     *            The {@link AbstractRunningQuery}.
     * 
     * @return The {@link AbstractRunningQuery} -or- another
     *         {@link AbstractRunningQuery} iff one exists with the same
     *         {@link UUID}.
     */
    protected AbstractRunningQuery putIfAbsent(final UUID queryId,
            final AbstractRunningQuery runningQuery) {

        if (queryId == null)
            throw new IllegalArgumentException();

		if (runningQuery == null)
			throw new IllegalArgumentException();

		// First, check [runningQueries] w/o acquiring a lock.
		{
			final AbstractRunningQuery tmp = runningQueries.get(queryId);

			if (tmp != null) {
		
				// Found existing query.
				return tmp;
				
			}
			
		}

		/*
		 * A lock is used to address a race condition here with the concurrent
		 * registration and halt of a query.
		 */

		lock.lock();
		
		try {
			
			// Test for a recently terminated query.
			final Future<Void> doneQueryFuture = doneQueries.get(queryId);

			if (doneQueryFuture != null) {

				// Throw out an appropriate exception for a halted query.
				handleDoneQuery(queryId, doneQueryFuture);

				// Should never get here.
        		throw new AssertionError();
        		
			}

			// Test again for an active query while holding the lock.
			final AbstractRunningQuery tmp = runningQueries.putIfAbsent(queryId,
					runningQuery);
			
			if (tmp != null) {
				
				// Another thread won the race.
				return tmp;
				
			}

			// Query was newly registered.
			return runningQuery;
			
		} finally {

			lock.unlock();
			
		}

    }

	/**
	 * Return the {@link AbstractRunningQuery} associated with that query
	 * identifier.
	 * 
	 * @param queryId
	 *            The query identifier.
	 * 
	 * @return The {@link AbstractRunningQuery} -or- <code>null</code> if there
	 *         is no query associated with that query identifier.
	 */
    public /*protected*/ AbstractRunningQuery getRunningQuery(final UUID queryId) {

        if(queryId == null)
            throw new IllegalArgumentException();

        AbstractRunningQuery q;

        /*
		 * First, test the concurrent map w/o obtaining a lock. This handles
		 * queries which are actively running.
		 */
		if ((q = runningQueries.get(queryId)) != null) {

			// Found running query.
        	return q;
        	
        }

		/*
		 * Since the query was not found in the set of actively running queries,
		 * we now get the lock, re-verify that it is not an active query, and
		 * verify that it is not a halted query.
		 */
        
		lock.lock();
        
		try {

        	if ((q = runningQueries.get(queryId)) != null) {
			
        		// Unlikely concurrent register of the query.
            	return q;
            
        	}

        	// Test to see if the query is halted.
			final Future<Void> doneQueryFuture = doneQueries.get(queryId);

			if (doneQueryFuture != null) {
				
				// Throw out an appropriate exception for a halted query.
				handleDoneQuery(queryId, doneQueryFuture);

				// Should never get here.
        		throw new AssertionError();
        		
        	}
			
            // Not found in done queries either.
            return null;
            
        } finally {
        	
        	lock.unlock();
        	
        }
        
    }

    /**
     * The query is no longer running. Resources associated with the query
     * should be released.
     */
    protected void halt(final AbstractRunningQuery q) {

    	lock.lock();

    	try {

    		// insert/touch the LRU of recently finished queries.
			doneQueries.put(q.getQueryId(), q.getFuture());

			// remove from the set of running queries.
			runningQueries.remove(q.getQueryId(), q);

			if(runningQueries.isEmpty()) {

				// Signal that no queries are running.
				nothingRunning.signalAll();
				
			}
			
		} finally {

			lock.unlock();
			
    	}

    }
    
	/**
	 * Handle a recently halted query by throwing an appropriate exception.
	 * 
	 * @param queryId
	 *            The query identifier.
	 * @param doneQueryFuture
	 *            The {@link Future} for that query from {@link #doneQueries}.
	 * @throws InterruptedException
	 *             if the query halted normally.
	 * @throws RuntimeException
	 *             if the query halted with an error.
	 */
    private void handleDoneQuery(final UUID queryId,final Future<Void> doneQueryFuture) {
        try {
        	// Check the Future.
			doneQueryFuture.get();
			// The query is done, so the caller can not access it any more.
			throw new InterruptedException();
		} catch (InterruptedException e) {
			/*
			 * Interrupted awaiting the future (note that the Future should be
			 * available immediately).
			 */
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			/*
			 * Some kind of an error when running the query (might have just
			 * been interrupted, in which case the InterruptedException will be
			 * wrapped here).
			 */
			throw new RuntimeException(e);
		}
    }

    /*
     * RunningQuery factory.
     */
    
    /**
     * Factory for {@link IRunningQuery}s.
     * 
     * @see Annotations#RUNNING_QUERY_CLASS
     */
    @SuppressWarnings("unchecked")
	protected AbstractRunningQuery newRunningQuery(
            /*final QueryEngine queryEngine,*/ final UUID queryId,
            final boolean controller, final IQueryClient clientProxy,
            final PipelineOp query) {

        final String className = query.getProperty(
                Annotations.RUNNING_QUERY_CLASS,
                Annotations.DEFAULT_RUNNING_QUERY_CLASS);

        final Class<IRunningQuery> cls;
        try {
            cls = (Class<IRunningQuery>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Bad option: "
                    + Annotations.RUNNING_QUERY_CLASS, e);
        }

        if (!IRunningQuery.class.isAssignableFrom(cls)) {
            throw new RuntimeException(Annotations.RUNNING_QUERY_CLASS
                    + ": Must extend: " + IRunningQuery.class.getName());
        }

        final IRunningQuery runningQuery;
        try {

            final Constructor<? extends IRunningQuery> ctor = cls
                    .getConstructor(new Class[] { QueryEngine.class,
                            UUID.class, Boolean.TYPE, IQueryClient.class,
                            PipelineOp.class });

            // save reference.
            runningQuery = ctor.newInstance(new Object[] { this, queryId,
                    controller, clientProxy, query });

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }
        
        /*
         * @todo either modify to allow IRunningQuery return or update the
         * javadoc to specify the AbstractRunningQuery base class. 
         * 
         * @todo Measure the runtime cost of this dynamic decision.  We could
         * always hardware the default.
         */
        return (AbstractRunningQuery) runningQuery;
        
//        return new ChunkedRunningQuery(this, queryId, true/* controller */,
//                this/* clientProxy */, query);

    }

	public UUID[] getRunningQueries() {

		return runningQueries.keySet().toArray(new UUID[0]);

	}
	
}
