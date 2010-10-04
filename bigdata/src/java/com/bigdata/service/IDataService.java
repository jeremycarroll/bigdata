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
package com.bigdata.service;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.Options;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.resources.StoreManager;
import com.bigdata.service.ndx.ClientIndexView;
import com.bigdata.service.ndx.DataServiceTupleIterator;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.Util;

/**
 * <p>
 * The data service interface provides remote access to named indices, provides
 * for both unisolated and isolated operations on those indices, and exposes the
 * {@link ITxCommitProtocol} interface to the {@link ITransactionManagerService}
 * service for the coordination of distributed transactions. Clients normally
 * write to the {@link IIndex} interface. The {@link ClientIndexView} provides
 * an implementation of that interface supporting range partitioned scale-out
 * indices which transparently handles lookup of data services in the metadata
 * index and mapping of operations across the appropriate data services.
 * </p>
 * <p>
 * Indices are identified by name. Scale-out indices are broken into index
 * partitions, each of which is a named index hosted on a data service. The name
 * of an index partition is given by
 * {@link Util#getIndexPartitionName(String, int)}. Clients are
 * <em>strongly</em> encouraged to use the {@link ClientIndexView} which
 * encapsulates lookup and distribution of operations on range partitioned
 * scale-out indices.
 * </p>
 * <p>
 * The data service exposes both fully isolated read-write transactions,
 * read-only transactions, lightweight read-historical operations, and
 * unisolated operations on named indices. These choices are captured by the
 * timestamp associated with the operation. When it is a transaction, this is
 * also known as the transaction identifier or <i>tx</i>. The following
 * distinctions are available:
 * <dl>
 * 
 * <dt>Unisolated</dt>
 * 
 * <dd>
 * <p>
 * Unisolated operation specify {@link ITx#UNISOLATED} as their transaction
 * identifier. Unisolated operations are ACID, but their scope is limited to the
 * commit group on the data service where the operation is executed. Unisolated
 * operations correspond more or less to read-committed semantics except that
 * writes are immediately visible to other operations in the same commit group.
 * </p>
 * <p>
 * Unisolated operations that allow writes obtain an exclusive lock on the live
 * version of the named index for the duration of the operation. Unisolated
 * operations that are declared as read-only read from the last committed state
 * of the named index and therefore do not compete with read-write unisolated
 * operations. This allows unisolated read operations to achieve higher
 * concurrency. The effect is as if the unisolated read operation runs before
 * the unisolated writes in a given commit group since the impact of those
 * writes are not visible to unisolated readers until the next commit point.
 * </p>
 * <p>
 * Unisolated write operations MAY be used to achieve "auto-commit" semantics
 * when distributed transactions are not required. Fully isolated transactions
 * are useful when multiple operations must be composed into a ACID unit.
 * </p>
 * <p>
 * While unisolated operations on a single data service are ACID, clients
 * generally operate against scale-out indices having multiple index partitions
 * hosted on multiple data services. Therefore client MUST NOT assume that an
 * unisolated operation described by the client against a scale-out index will
 * be ACID when that operation is distributed across the various index
 * partitions relevant to the client's request. In practice, this means that
 * contract for ACID unisolated operations is limited to either: (a) operations
 * where the data is located on a single data service instance; or (b)
 * unisolated operations that are inherently designed to achieve a
 * <em>consistent</em> result. Sometimes it is sufficient to configure a
 * scale-out index such that index partitions never split some logical unit -
 * for example, the {schema + primaryKey} for a {@link SparseRowStore}, thereby
 * obtaining an ACID guarentee since operations on a logical row will always
 * occur within the same index partition.
 * </p>
 * </dd>
 * 
 * <dt>Light weight historical reads</dt>
 * 
 * <dd>Historical reads are indicated using <code>tx</code>, where <i>tx</i>
 * is a timestamp and is associated with the closest commit point LTE to the
 * timestamp. A historical read is fully isolated but has very low overhead and
 * does NOT require the caller to open the transaction. The read will have a
 * consistent view of the data as of the most recent commit point not greater
 * than <i>tx</i>. Unlike a distributed read-only transaction, a historical
 * read does NOT impose a distributed read lock. While the operation will have
 * access to the necessary resources on the local data service, it is possible
 * that resources for the same timestamp will be concurrently released on other
 * data services. If you need to map a read operation across the distributed
 * database, the you must use a read only transaction which will assert the
 * necessary read-lock.</dd>
 * 
 * <dt>Distributed transactions</dt>
 * 
 * <dd>Distributed transactions are coordinated using an
 * {@link ITransactionManagerService} service and incur more overhead than both
 * unisolated and historical read operations. Transactions are assigned a start
 * time (the transaction identifier) when they begin and must be explicitly
 * closed by either an abort or a commit. Both read-only and read-write
 * transactions assert read locks which force the retention of resources
 * required for a consistent view as of the transaction start time until the
 * transaction is closed.</dd>
 * </dl>
 * </p>
 * <p>
 * Implementations of this interface MUST be thread-safe. Methods declared by
 * this interface MUST block for each operation. Client operations SHOULD be
 * buffered by a thread pool with a FIFO policy so that client requests may be
 * decoupled from data service operations and clients may achieve greater
 * parallelism.
 * </p>
 * 
 * <h2>Index Partitions: Split, Join, and Move</h2>
 * 
 * <p>
 * 
 * Scale-out indices are broken tranparently down into index partitions. When a
 * scale-out index is initially registered, one or more index partitions are
 * created and registered on one or more data services.
 * </p>
 * 
 * <p>
 * 
 * Note that each index partitions is just an {@link IIndex} registered under
 * the name assigned by {@link Util#getIndexPartitionName(String, int)}
 * and whose {@link IndexMetadata#getPartitionMetadata()} returns a description
 * of the resources required to compose a view of that index partition from the
 * resources located on a {@link DataService}. The shard service will
 * respond for that index partition IFF there is an index under that name
 * registered on the shard service as of the <i>timestamp</i> associated
 * with the request. If the index is not registered then a
 * {@link NoSuchIndexException} will be thrown. If the index was registered and
 * has since been split, joined or moved then a {@link StaleLocatorException}
 * will be thrown (this will occur only for index partitions of scale-out
 * indices). <strong>All methods on this and derived interfaces which are
 * defined for an index name and timestamp MUST conform to these semantics.</strong>
 * 
 * </p>
 * 
 * <p>
 * 
 * As index partitions grow in size they may be <em>split</em> into 2 or more
 * index partitions covering the same key range as the original index partition.
 * When this happens a new index partition identifier is assigned by the
 * metadata service to each of the new index partitions and the old index
 * partition is retired in an atomic operation. A similar operation can
 * <em>move</em> an index partition to a different shard service in
 * order to load balance a federation. Finally, when two index partitions shrink
 * in size, they maybe moved to the same shard service and an atomic
 * <i>join</i> operation may re-combine them into a single index partition
 * spanning the same key range.
 * 
 * </p>
 * 
 * <p>
 * 
 * Split, join, and move operations all result in the old index partition being
 * dropped on the shard service. Clients having a stale
 * {@link PartitionLocator} record will attempt to reach the now defunct index
 * partition after it has been dropped and will receive a
 * {@link StaleLocatorException}.
 * 
 * </p>
 * 
 * 
 * <h2>{@link StaleLocatorException}</h2>
 * 
 * <p>
 * 
 * Clients of the shard service MUST handle this exception by refreshing their
 * cached {@link PartitionLocator} for the key range associated with the index
 * partition which they wish to query and then re-issuing their request. By
 * following this simple rule the client will automatically handle index
 * partition splits, joins, and moves without error and in a manner which is
 * completely transparent to the application. Note that splits, joins, and moves
 * DO NOT alter the {@link PartitionLocator} for historical reads, only for
 * ongoing writes. This exception is generally (but not always) wrapped.
 * Applications typically DO NOT write directly to this shard/data service
 * interface and therefore DO NOT need to worry about this. See
 * {@link ClientIndexView}, which automatically handles this exception.
 * 
 * </p>
 * 
 * <h2>{@link IOException}</h2>
 * 
 * <p>
 * 
 * All methods on this and derived interfaces can throw an {@link IOException}.
 * In all cases an <em>unwrapped</em> exception that is an instance of
 * {@link IOException} indicates an error in the Remote Method Invocation (RMI)
 * layer.
 * 
 * </p>
 * 
 * <h2>{@link ExecutionException} and {@link InterruptedException}</h2>
 * 
 * <p>
 * 
 * An <em>unwrapped</em> {@link ExecutionException} or
 * {@link InterruptedException} indicates a problem when running the request as
 * a task in the {@link IConcurrencyManager} on the shard service. The
 * exception always wraps a root cause which may indicate the underlying
 * problem. Methods which do not declare these exceptions are not run under the
 * {@link IConcurrencyManager}.
 * 
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @todo add support for triggers? unisolated triggers must be asynchronous if
 *       they will take actions with high latency (such as writing on a
 *       different index partition, which could be remote). Low latency actions
 *       might include emitting asynchronous messages. transactional triggers
 *       can have more flexibility since they are under less of a latency
 *       constraint.
 */
//BTM public interface IDataService extends ITxCommitProtocol, IService, IRemoteExecutor {
public interface IDataService extends ShardService,
                                      ShardManagement,
                                      OverflowAdmin,
//BTM - ShardService extends this          ITxCommitProtocol,
                                      IService
//BTM - PRE_FRED_3481                 ,
//BTM - PRE_FRED_3481                                      IRemoteExecutor
{

}
