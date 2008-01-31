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
 * Created on Jan 10, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.IEntryFilter;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IResultHandler;
import com.bigdata.btree.RangeCountProcedure;
import com.bigdata.btree.IIndexProcedure.IIndexProcedureConstructor;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;

/**
 * A view onto an unpartitioned index living inside an embedded
 * {@link DataService}. Access to the index is moderated by the concurrency
 * control mechanisms of the data service.
 * <p>
 * Applications writing to this interface are directly portable to scale-out
 * partitioned indices and to embedded indices without concurrency control.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServiceIndex implements IIndex {

    /**
     * 
     */
    private static final String NON_BATCH_API = "Non-batch API";

    /**
     * Note: Invocations of the non-batch API are logged at the WARN level since
     * they result in an application that can not scale efficiently to
     * partitioned indices.
     */
    protected static final Logger log = Logger.getLogger(DataServiceIndex.class);
    
    private final String name;
    private final long tx;
    private final EmbeddedDataService dataService;

    /**
     * The transaction identifier for this index view -or-
     * {@link ITx#UNISOLATED} if the index view is not transactional.
     * 
     * @return The transaction identifier for the index view.
     */
    public long getTx() {
        
        return tx;
        
    }
    
    /**
     * The name of the scale-out index.
     */
    public String getName() {
        
        return name;
        
    }
    
    /**
     * The capacity for the range query iterator.
     * 
     * @todo should be a ctor option.
     */
    private final int capacity = 100000;

    /**
     * This may be used to disable the non-batch API, which is quite convenient
     * for location code that needs to be re-written to use
     * {@link IIndexProcedure}s.
     * 
     * @todo make this a config option and also support this option on the
     *       bigdata clients.
     */
    private final boolean batchOnly = false;
    
    /**
     * Creates a view onto an unpartitioned index living on an embedded data
     * service.
     * 
     * @param name
     *            The index name.
     * @param tx
     *            The transaction -or- {@link ITx#UNISOLATED} to use
     *            unisolated atomic index operations.
     * @param dataService
     *            The data service.
     */
    public DataServiceIndex(String name, long tx, EmbeddedDataService dataService) {

        this.name = name;
        
        this.tx = tx;
        
        this.dataService = dataService;
        
    }
    
    public UUID getIndexUUID() {

        try {

            return dataService.getIndexUUID(name);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public String getStatistics() {

        try {

            return dataService.getStatistics(name);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public boolean isIsolatable() {

        final IIndex ndx = dataService.getJournal().getIndex(name);
        
        if(ndx == null) {
            
            throw new NoSuchIndexException(name);
            
        }
        
        return ndx.isIsolatable();

    }

    public boolean contains(byte[] key) {
        
        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final BatchContains proc = new BatchContains(//
                1, // n,
                0, // offset
                new byte[][] { (byte[]) key } // keys
        );

        final boolean[] ret = ((ResultBitBuffer) submit((byte[]) key, proc))
                .getResult();

        return ret[0];
        
    }

    public Object insert(byte[] key, Object value) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final BatchInsert proc = new BatchInsert(//
                1, // n,
                0, // offset
                new byte[][] { key }, // keys
                new byte[][] { (byte[]) value }, // vals
                true // returnOldValues
        );

        final byte[][] ret = ((ResultBuffer) submit(key, proc)).getResult();

        return ret[0];

    }

    public Object lookup(byte[] key) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final BatchLookup proc = new BatchLookup(//
                1, // n,
                0, // offset
                new byte[][] { key } // keys
        );

        final byte[][] ret = ((ResultBuffer)submit(key, proc)).getResult();

        return ret[0];

    }

    public Object remove(byte[] key) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final BatchRemove proc = new BatchRemove(//
                1, // n,
                0, // offset
                new byte[][] { key }, // keys
                true // returnOldValues
        );

        final byte[][] ret = ((ResultBuffer)submit(key, proc)).getResult();

        return ret[0];

    }

    /**
     * Submits an index procedure to the embedded data service returning the
     * result of that procedure.
     * <p>
     * Note: This is parallel to
     * {@link ClientIndexView#submit(byte[], IIndexProcedure)} so that some code
     * can be shared by both classes.
     * 
     * @param key
     *            The key (unused).
     * @param proc
     *            The procedure.
     * 
     * @return The value returned by {@link IIndexProcedure#apply(IIndex)}
     */
    public Object submit(byte[] key,IIndexProcedure proc) {

        try {

            return dataService.submit(tx, name, proc);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    public long rangeCount(byte[] fromKey, byte[] toKey) {

//        final LongAggregator handler = new LongAggregator();
        
        final RangeCountProcedure proc = new RangeCountProcedure(fromKey,toKey);

        final long rangeCount;

        try {

            rangeCount = (Long) dataService.submit(tx,name,proc);
        
        } catch (Exception ex) {
        
            throw new RuntimeException(ex);
        
        }

        return rangeCount;

    }
    
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return rangeIterator(fromKey, toKey, capacity, IRangeQuery.KEYS
                | IRangeQuery.VALS, null/* filter */);

    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey, int capacity, int flags, IEntryFilter filter) {

        return new RangeQueryIterator(dataService, name, tx, fromKey, toKey,
                capacity, flags, filter);

    }

    public void submit(int n, byte[][] keys, byte[][] vals,
            IIndexProcedureConstructor ctor, IResultHandler aggregator) {

        try {

            Object result = dataService.submit(tx, name, ctor.newInstance(
                    n, 0/* offset */, keys, vals));
            
            aggregator.aggregate(result, new Split(null,0,n));

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

}
