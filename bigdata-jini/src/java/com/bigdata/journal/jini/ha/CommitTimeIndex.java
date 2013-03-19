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
package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.ChecksumUtility;

/**
 * {@link BTree} mapping <em>commitTime</em> (long integers) to
 * {@link IRootBlockView} records.
 * <p>
 * Note: Access to this object MUST be synchronized.
 * <p>
 * Note: This is used as a transient data structure that is populated from the
 * file system by the {@link HAJournalServer}. A separate instance is maintained
 * for the HALog files and the snapshot files.
 */
public class CommitTimeIndex extends BTree {

    /**
     * Instance used to encode the timestamp into the key.
     */
    final private IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);

    /**
     * Create a transient instance.
     * 
     * @return The new instance.
     */
    static public CommitTimeIndex createTransient() {
    
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBTreeClassName(CommitTimeIndex.class.getName());

        metadata.setTupleSerializer(new TupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG)));

        return (CommitTimeIndex) BTree.createTransient(/*store, */metadata);
        
    }

    /**
     * Load from the store.
     * 
     * @param store
     *            The backing store.
     * @param checkpoint
     *            The {@link Checkpoint} record.
     * @param metadata
     *            The metadata record for the index.
     */
    public CommitTimeIndex(final IRawStore store, final Checkpoint checkpoint,
            final IndexMetadata metadata, final boolean readOnly) {

        super(store, checkpoint, metadata, readOnly);

    }
    
    /**
     * Encodes the commit time into a key.
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @return The corresponding key.
     */
    private byte[] getKey(final long commitTime) {

        return keyBuilder.reset().append(commitTime).getKey();

    }

    /**
     * Return the {@link IRootBlock} identifying the journal having the largest
     * commitTime that is less than or equal to the given timestamp. This is
     * used primarily to locate the commit record that will serve as the ground
     * state for a transaction having <i>timestamp</i> as its start time. In
     * this context the LTE search identifies the most recent commit state that
     * not later than the start time of the transaction.
     * 
     * @param timestamp
     *            The given timestamp.
     * 
     * @return The description of the relevant journal resource -or-
     *         <code>null</code> iff there are no journals in the index that
     *         satisify the probe.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than or equals to ZERO (0L).
     */
    synchronized public IRootBlockView find(final long timestamp) {

        if (timestamp <= 0L)
            throw new IllegalArgumentException();
        
        // find (first less than or equal to).
        final long index = findIndexOf(timestamp);
        
        if(index == -1) {
            
            // No match.
            log.warn("Not found: " + timestamp);
            
            return null;
            
        }

        return valueAtIndex(index);
        
    }

    /**
     * Retrieve the entry from the index.
     */
    private IRootBlockView valueAtIndex(final long index) {

        final byte[] val = super.valueAt(index);

        assert val != null : "Entry has null value: index=" + index;
        
        final IRootBlockView entry = new RootBlockView(false/* rootBlock0 */,
                ByteBuffer.wrap(val), ChecksumUtility.getCHK());

        return entry;

    }
    
    /**
     * Find the first journal whose <em>createTime</em> is strictly greater
     * than the timestamp.
     * 
     * @param timestamp
     *            The timestamp. A value of ZERO (0) may be used to find the
     *            first journal.
     * 
     * @return The commit record -or- <code>null</code> if there is no commit
     *         record whose timestamp is strictly greater than <i>timestamp</i>.
     */
    synchronized public IRootBlockView findNext(final long timestamp) {

        /*
         * Note: can also be written using rangeIterator().next().
         */
        
        if (timestamp < 0L)
            throw new IllegalArgumentException();
        
        // find first strictly greater than.
        final long index = findIndexOf(Math.abs(timestamp)) + 1;
        
        if (index == nentries) {

            // No match.

            return null;
            
        }
        
        return valueAtIndex(index);

    }

    /**
     * Find the index of the {@link ICommitRecord} having the largest timestamp
     * that is less than or equal to the given timestamp.
     * 
     * @return The index of the {@link ICommitRecord} having the largest
     *         timestamp that is less than or equal to the given timestamp -or-
     *         <code>-1</code> iff there are no {@link ICommitRecord}s
     *         defined.
     */
    synchronized public long findIndexOf(final long timestamp) {
        
        long pos = super.indexOf(getKey(timestamp));
        
        if (pos < 0) {

            /*
             * the key lies between the entries in the index, or possible before
             * the first entry in the index. [pos] represents the insert
             * position. we convert it to an entry index and subtract one to get
             * the index of the first commit record less than the given
             * timestamp.
             */
            
            pos = -(pos+1);

            if(pos == 0) {

                // No entry is less than or equal to this timestamp.
                return -1;
                
            }
                
            pos--;

            return pos;
            
        } else {
            
            /*
             * exact hit on an entry.
             */
            
            return pos;
            
        }

    }
    
    /**
     * Add an entry under the commitTime associated with the
     * {@link IRootBlockView} record.
     * 
     * @param rootBlock
     *            The {@link IRootBlockView} record.
     * 
     * @exception IllegalArgumentException
     *                if <i>commitTime</i> is <code>0L</code>.
     * @exception IllegalArgumentException
     *                if <i>rootBLock</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if there is already an entry registered under for the
     *                given timestamp.
     */
    synchronized public void add(final IRootBlockView rootBlock) {

        if (rootBlock == null)
            throw new IllegalArgumentException();
        
        final long createTime = rootBlock.getLastCommitTime();

        if (createTime == 0L)
            throw new IllegalArgumentException();

        final byte[] key = getKey(createTime);
        
        if(super.contains(key)) {
            
            throw new IllegalArgumentException("entry exists: timestamp="
                    + createTime);
            
        }
        
        // add a serialized entry to the persistent index.
        super.insert(key, BytesUtil.getBytes(rootBlock.asReadOnlyBuffer()));

    }
    
    /**
     * Find the commit counter for the most recent snapshot (if any).
     * 
     * @return That commit counter -or- ZERO (0L) if there are no snapshots.
     */
    public long getMostRecentSnapshotCommitCounter() {
        
        final long snapshotCommitCounter;
        synchronized (this) {

            final ITupleIterator<IRootBlockView> itr = 
                    rangeIterator(null/* fromKey */, null/* toKey */,
                            1/* capacity */, IRangeQuery.DEFAULT
                                    | IRangeQuery.REVERSE/* flags */, null/* filter */);

            if (itr.hasNext()) {

                final ITuple<IRootBlockView> t = itr.next();
                
                final IRootBlockView rootBlock = t.getObject();

                snapshotCommitCounter = rootBlock.getCommitCounter();

            } else {

                snapshotCommitCounter = 0L;

            }

        }

        return snapshotCommitCounter;
        
    }
    
    /**
     * Encapsulates key and value formation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id: JournalIndex.java 5892 2012-01-27 13:22:11Z thompsonbry $
     */
    static protected class TupleSerializer extends
            DefaultTupleSerializer<Long, IRootBlockView> {

        /**
         * 
         */
        private static final long serialVersionUID = -2851852959439807542L;

        /**
         * De-serialization ctor.
         */
        public TupleSerializer() {

            super();
            
        }

        /**
         * Ctor when creating a new instance.
         * 
         * @param keyBuilderFactory
         */
        public TupleSerializer(final IKeyBuilderFactory keyBuilderFactory) {

            super(keyBuilderFactory);

        }
        
        /**
         * Decodes the key as a commit time.
         */
        @Override
        public Long deserializeKey(final ITuple tuple) {

            return KeyBuilder
                    .decodeLong(tuple.getKeyBuffer().array(), 0/* offset */);

        }

        /**
         * De-serializes an object from the {@link ITuple#getValue() value} stored
         * in the tuple (ignores the key stored in the tuple).
         */
        public IRootBlockView deserialize(final ITuple tuple) {

            if (tuple == null)
                throw new IllegalArgumentException();

            return (IRootBlockView) new RootBlockView(false/* rootBlock0 */,
                    ByteBuffer.wrap(tuple.getValue()), ChecksumUtility.getCHK());
            
        }

        /**
         * The initial version (no additional persistent state).
         */
        private final static transient byte VERSION0 = 0;

        /**
         * The current version.
         */
        private final static transient byte VERSION = VERSION0;

        public void readExternal(final ObjectInput in) throws IOException,
                ClassNotFoundException {

            super.readExternal(in);
            
            final byte version = in.readByte();
            
            switch (version) {
            case VERSION0:
                break;
            default:
                throw new UnsupportedOperationException("Unknown version: "
                        + version);
            }

        }

        public void writeExternal(final ObjectOutput out) throws IOException {

            super.writeExternal(out);
            
            out.writeByte(VERSION);
            
        }

    }

}
