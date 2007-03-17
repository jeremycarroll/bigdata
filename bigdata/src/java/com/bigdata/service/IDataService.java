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
 * Created on Mar 15, 2007
 */

package com.bigdata.service;

import java.util.concurrent.ExecutionException;

import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITxCommitProtocol;
import com.bigdata.journal.IsolationEnum;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.IBatchOp;
import com.bigdata.service.DataService.RangeQueryResult;

/**
 * Data service interface.
 * <p>
 * The data service exposes the methods on this interface to the client and the
 * {@link ITxCommitProtocol} methods to the {@link ITransactionManager} service.
 * <p>
 * The data service exposes both isolated (transactional) and unisolated batch
 * operations on scalable named btrees. Transactions are identified by their
 * start time. BTrees are identified by name. The btree batch API provides for
 * existence testing, lookup, insert, removal, and an extensible mutation
 * operation. Other operations exposed by this interface include: remote
 * procedure execution, key range traversal, and mapping of an operator over a
 * key range.
 * <p>
 * Unisolated processing is broken down into idempotent operation (reads) and
 * mutation operations (insert, remove, the extensible batch operator, and
 * remote procedure execution).
 * <p>
 * Unisolated writes are serialized and ACID. If an unisolated write succeeds,
 * then it will commit immediately. If the unisolated write fails, the partial
 * write on the journal will be discarded.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IDataService extends ITxCommitProtocol {

    /**
     * Used by the client to submit a batch operation on a named B+Tree
     * (synchronous).
     * <p>
     * Unisolated operations SHOULD be used to achieve "auto-commit" semantics.
     * Fully isolated transactions are useful IFF multiple operations must be
     * composed into a ACID unit.
     * <p>
     * While unisolated batch operations on a single data service are ACID,
     * clients are required to locate all index partitions for the logical
     * operation and distribute their operation across the distinct data service
     * instances holding the affected index partitions. In practice, this means
     * that contract for ACID unisolated operations is limited to operations
     * where the data is located on a single data service instance. For ACID
     * operations that cross multiple data service instances the client MUST use
     * a fully isolated transaction. While read-committed transactions impose
     * low system overhead, clients interested in the higher possible total
     * throughput SHOULD choose unisolated read operations in preference to a
     * read-committed transaction.
     * <p>
     * This method is thread-safe. It will block for each operation. It should
     * be invoked within a pool request handler threads servicing a network
     * interface and thereby decoupling data service operations from client
     * requests. When using as part of an embedded database, the client
     * operations MUST be buffered by a thread pool with a FIFO policy so that
     * client requests may be decoupled from data service operations.
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
    public void batchOp(long tx, String name, IBatchOp op)
            throws InterruptedException, ExecutionException;

    /**
     * Submit a procedure.
     * <p>
     * <p>
     * Unisolated operations SHOULD be used to achieve "auto-commit" semantics.
     * Fully isolated transactions are useful IFF multiple operations must be
     * composed into a ACID unit.
     * <p>
     * While unisolated batch operations on a single data service are ACID,
     * clients are required to locate all index partitions for the logical
     * operation and distribute their operation across the distinct data service
     * instances holding the affected index partitions. In practice, this means
     * that contract for ACID unisolated operations is limited to operations
     * where the data is located on a single data service instance. For ACID
     * operations that cross multiple data service instances the client MUST use
     * a fully isolated transaction. While read-committed transactions impose
     * low system overhead, clients interested in the higher possible total
     * throughput SHOULD choose unisolated read operations in preference to a
     * read-committed transaction.
     * 
     * @param tx
     *            The transaction identifier -or- zero (0L) IFF the operation is
     *            NOT isolated by a transaction.
     * @param proc
     *            The procedure to be executed.
     *            
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void submit(long tx, IProcedure proc) throws InterruptedException,
            ExecutionException;
    
    /**
     * Streaming traversal of keys and/or values in a given key range.
     * <p>
     * Note: The rangeQuery operation is NOT allowed for either unisolated reads
     * or read-committed transactions (the underlying constraint is that the
     * {@link BTree} does NOT support traversal under concurrent modification
     * this operation is limited to read-only or fully isolated transactions).
     * 
     * @param tx
     * @param name
     * @param fromKey
     * @param toKey
     * @param flags (@todo define flags: count yes/no, keys yes/no, values yes/no)
     * 
     * @exception InterruptedException
     *                if the operation was interrupted (typically by
     *                {@link #shutdownNow()}.
     * @exception ExecutionException
     *                If the operation caused an error. See
     *                {@link ExecutionException#getCause()} for the underlying
     *                error.
     * @exception UnsupportedOperationException
     *                If the tx is zero (0L) (indicating an unisolated
     *                operation) -or- if the identifed transaction is
     *                {@link IsolationEnum#ReadCommitted}.
     */
    public RangeQueryResult rangeQuery(long tx, String name, byte[] fromKey,
            byte[] toKey, int flags) throws InterruptedException, ExecutionException;

//    /**
//     * Execute a map worker task against all key/value pairs in a key range,
//     * writing the results onto N partitions of an intermediate file.
//     */
//    public void map(long tx, String name, byte[] fromKey, byte[] toKey,
//            IMapOp op) throws InterruptedException, ExecutionException;

}
