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
 * Created on Feb 27, 2008
 */

package com.bigdata.service.ndx;

import java.io.IOException;
import java.util.Iterator;

import com.bigdata.btree.AbstractChunkedTupleIterator;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.BatchRemove.BatchRemoveConstructor;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IBlock;
import com.bigdata.service.AbstractDistributedFederation;
//BTM import com.bigdata.service.IDataService;

//BTM
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardManagement;
import com.bigdata.service.ShardService;

/**
 * Class supports range query across against an unpartitioned index on an
 * {@link ShardService} but DOES NOT handle index partition splits, moves or
 * joins.
 * <p>
 * Note: This class supports caching of the remote metadata index, which does
 * not use index partitions, by the {@link AbstractDistributedFederation}.
 * 
 * @todo write tests for read-consistent.
 */
public class RawDataServiceTupleIterator<E> extends AbstractChunkedTupleIterator<E> {
    
//    protected static final transient Logger log = Logger
//            .getLogger(RawDataServiceTupleIterator.class);
//
//    protected static final boolean INFO = log.isInfoEnabled();
    
    /**
     * Error message used by {@link #getKey()} when the iterator was not
     * provisioned to request keys from the data service.
     */
    static public transient final String ERR_NO_KEYS = "Keys not requested";
    
    /**
     * Error message used by {@link #getValue()} when the iterator was not
     * provisioned to request values from the data service.
     */
    static public transient final String ERR_NO_VALS = "Values not requested";
    
    /**
     * The data service for the index.
     * <p>
     * Note: Be careful when using this field since you take on responsibilty
     * for handling index partition splits, joins, and moves!
     * 
     * @todo this should failover if a data service dies. we need the logical
     *       data service UUID for that and we need to have (or discover) the
     *       service that maps logical data service UUIDs to physical data
     *       service UUIDs
     */
//BTM    protected final IDataService dataService;
//BTM

//BTM - BEGIN IDATA_SERVICE CHANGED TO SHARD_SERVICE
//BTM protected IDataService dataService = null;
//BTM private IDataService remoteShardMgr = null;

protected ShardService dataService = null;
protected ShardLocator metadataService = null;
private ShardManagement shardMgr = null;
//BTM - END IDATA_SERVICE CHANGED TO SHARD_SERVICE

    
    /**
     * The name of the index partition on which the range query is being
     * performed.
     */
    public final String name;
    
    /**
     * From the ctor.
     */
    private final long timestamp;

    /**
     * From the ctor.
     */
    private final boolean readConsistent;
    
    /**
     * 
     * @param dataService
     *            The data service on which the index resides.
     * @param name
     *            The name of the index partition on that data service.
     * @param timestamp
     *            The timestamp used for the reads.
     * @param readConsistent
     *            This option is only available for {@link ITx#READ_COMMITTED}.
     *            When <code>true</code>, the first read will be against the
     *            most recent commit point on the database and any continuation
     *            queries will be against the <em>same</em> commit point. When
     *            <code>false</code>, each read will be against the most
     *            recent commit point (so the data can be drawn from multiple
     *            commit points if there are concurrent commits).
     *            <p>
     *            The <i>readConsistent</i> option is a tweak available only at
     *            this low level for the {@link RawDataServiceTupleIterator}.
     *            It avoids a possible RMI to obtain the most recent global
     *            commit point using {@link IIndexStore#getLastCommitTime()} in
     *            favor of using the most recent commit point on the index
     *            partition at the time that the query is actually executed. If
     *            you are reading against a scale-out index, then similar
     *            effects are obtained by choosing either
     *            {@link ITx#READ_COMMITTED} or
     *            {@link IIndexStore#getLastCommitTime()}. See
     *            {@link ClientIndexView} and
     *            {@link PartitionedTupleIterator}.
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param flags
     * @param filter
     */
//BTM    public RawDataServiceTupleIterator(final IDataService dataService,
public RawDataServiceTupleIterator(final ShardService dataService,
            final String name, final long timestamp,
            final boolean readConsistent, final byte[] fromKey,
            final byte[] toKey, final int capacity, final int flags,
            final IFilterConstructor filter) {

        super(fromKey, toKey, capacity, flags, filter);
        
System.out.println("\n>>>> RawDataServiceTupleIterator: dataService = "+dataService);
        if (dataService == null) {
System.out.println("\n"+com.bigdata.util.Util.getCurrentStackTrace());
            throw new IllegalArgumentException("null shard service");
        }

        if (name == null) {
            throw new IllegalArgumentException("null name");
        }
        
        if (capacity < 0) {
            throw new IllegalArgumentException
                          ("capacity < 0"+" ["+capacity+"]");
        }

//        if (timestamp == ITx.UNISOLATED && readConsistent) {
//            
//            throw new IllegalArgumentException(
//                    "Read-consistent not available for unisolated operations");
//            
//        }

        this.dataService = dataService;
        this.name = name;
        this.timestamp = timestamp;
        this.readConsistent = readConsistent;

//BTM
this.shardMgr = (ShardManagement)(this.dataService);
    }

//BTM - BEGIN ADDED NEW CONSTRUCTOR FOR SHARD_LOCATOR
public RawDataServiceTupleIterator(final ShardLocator metadataService,
            final String name, final long timestamp,
            final boolean readConsistent, final byte[] fromKey,
            final byte[] toKey, final int capacity, final int flags,
            final IFilterConstructor filter) {

        super(fromKey, toKey, capacity, flags, filter);
        
        if (metadataService == null) {
            throw new IllegalArgumentException("null shard locator");
        }
        if (name == null) {
            throw new IllegalArgumentException("null name");
        }
        
        if (capacity < 0) {
            throw new IllegalArgumentException
                          ("capacity < 0"+" ["+capacity+"]");
        }

        this.metadataService = metadataService;
        this.name = name;
        this.timestamp = timestamp;
        this.readConsistent = readConsistent;

//BTM - BEGIN IDATA_SERVICE CHANGED TO SHARD_SERVICE
//BTM if(this.metadataService instanceof IDataService) {
//BTM     remoteShardMgr = (IDataService)(this.metadataService);
//BTM } else if(this.metadataService instanceof ShardManagement) {
//BTM     shardMgr = (ShardManagement)(this.metadataService);
//BTM }

this.shardMgr = (ShardManagement)(this.metadataService);
//BTM - END IDATA_SERVICE CHANGED TO SHARD_SERVICE

    }
//BTM - END ADDED NEW CONSTRUCTOR FOR SHARD_LOCATOR

    /**
     * Atomic operation caches a chunk of results from an {@link ShardService}.
     * <P>
     * Note: This uses the <i>timestamp</i> specified by the caller NOT the
     * {@link #timestamp} value stored by this class. This allows us to have
     * read-consistent semantics if desired for {@link ITx#UNISOLATED} or 
     * {@link ITx#READ_COMMITTED} operations.
     */
    @Override
    protected ResultSet getResultSet(final long timestamp,
            final byte[] fromKey, final byte[] toKey, final int capacity,
            final int flags, final IFilterConstructor filter) {

        if (INFO) {
//BTM - BEGIN
if(metadataService != null) {
    log.info("name=" + name + ", fromKey="
                    + BytesUtil.toString(fromKey) + ", toKey="
                    + BytesUtil.toString(toKey) + ", shardLocator="
                    + metadataService);
} else {
            log.info("name=" + name + ", fromKey="
                    + BytesUtil.toString(fromKey) + ", toKey="
                    + BytesUtil.toString(toKey) + ", dataService="
                    + dataService);
}
//BTM - END
        }

        try {

//BTM - BEGIN
//BTM - BEGIN IDATA_SERVICE CHANGED TO SHARD_SERVICE
//BTM if(metadataService != null) {
//BTM     if(remoteShardMgr != null) {
//BTM         return remoteShardMgr.rangeIterator(timestamp, name, fromKey, toKey, capacity, flags, filter);
//BTM     } else if(shardMgr != null) {
//BTM         return shardMgr.rangeIterator(timestamp, name, fromKey, toKey, capacity, flags, filter);
//BTM     } else {
//BTM         throw new RuntimeException("RawDataServiceTupleIterator.getResultSet: shard locator (metadata) service is not a shard manager [type="+metadataService.getClass()+"]");
//BTM     }

//BTM            return dataService.rangeIterator(timestamp, name, fromKey, toKey, capacity, flags, filter);
return shardMgr.rangeIterator(timestamp, name, fromKey, toKey, capacity, flags, filter);

//BTM - END IDATA_SERVICE CHANGED TO SHARD_SERVICE
//BTM - END
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }
        
    @Override
    protected void deleteBehind(final int n, final Iterator<byte[]> itr) {

        final byte[][] keys = new byte[n][];
        
        int i = 0;
        
        while(itr.hasNext()) {
            
            keys[i] = itr.next();
            
        }
        
        try {

            /*
             * Note: default key serializer is used : @todo why? because the
             * IndexMetadata is not on hand locally?
             */
//BTM            dataService.submit(timestamp, name, BatchRemoveConstructor.RETURN_MUTATION_COUNT.newInstance(0/* fromIndex */, n/* toIndex */, keys, null/*vals*/)).get();
((ShardManagement)dataService).submit(timestamp, name, BatchRemoveConstructor.RETURN_MUTATION_COUNT.newInstance(0/* fromIndex */, n/* toIndex */, keys, null/*vals*/)).get();

        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }

    }

    @Override
    protected void deleteLast(final byte[] key) {

        try {
            
            // Note: default key serializer is used.
//BTM            dataService.submit(timestamp, name, BatchRemoveConstructor.RETURN_MUTATION_COUNT.newInstance(0/* fromIndex */, 1/* toIndex */, new byte[][] { key }, null/*vals*/)).get();
((ShardManagement)dataService).submit(timestamp, name, BatchRemoveConstructor.RETURN_MUTATION_COUNT.newInstance(0/* fromIndex */, 1/* toIndex */, new byte[][] { key }, null/*vals*/)).get();

        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    @Override
    protected IBlock readBlock(final int sourceIndex, final long addr) {
        
        final IResourceMetadata resource = rset.getSources()[sourceIndex];
        
        try {
            
            return dataService.readBlock(resource, addr);
            
        } catch (IOException e) {

            throw new RuntimeException(e);
            
        }
        
    }

    @Override
    final protected long getTimestamp() {
     
        return timestamp;
        
    }

    @Override
    final protected boolean getReadConsistent() {
        
        return readConsistent;
        
    }
    
}
