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
package com.bigdata.service;

import com.bigdata.btree.BTree;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.CommitRecordIndex.Entry;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

/**
 * A {@link BTree} whose keys are event start times and whose values are the
 * serialized {@link Event}s.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class EventBTree {

    private UnisolatedReadWriteIndex btree;

    private static EventBTreeTupleSerializer tupleSer =
                new EventBTreeTupleSerializer(
                        new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG));

    public EventBTree(BTree btree) {
        this.btree = new UnisolatedReadWriteIndex(btree);
    }

    /**
     * Create a new instance.
     *
     * @param store
     *            The backing store.
     *
     * @return The new instance.
     */
    public static EventBTree create(final IRawStore store) {
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        metadata.setBTreeClassName(BTree.class.getName());
        metadata.setTupleSerializer(tupleSer);
        return new EventBTree(BTree.create(store, metadata));
    }

    /**
     * Create a new transient instance.
     */
    public static EventBTree createTransient() {
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        metadata.setBTreeClassName(BTree.class.getName());
        metadata.setTupleSerializer(tupleSer);
        return new EventBTree(BTree.createTransient(metadata));
    }

    /**
     * Count the number or keys between two time values.
     */
    public long rangeCount(final long fromTime, final long toTime) {
        return btree.rangeCount(encodeKey(fromTime), encodeKey(toTime));

    }

    public Iterator<Event> rangeIterator(final long fromTime, final long toTime) {
        byte[] fromKey = tupleSer.serializeKey(fromTime);
        byte[] toKey = tupleSer.serializeKey(toTime);
        return new EntryIterator(btree.rangeIterator(fromKey, toKey));
    }
    
    /**
     * An iterator mapping
     */
    private static class EntryIterator implements Iterator<Event> {
        private ITupleIterator<byte[]> tupleIterator;

        private EntryIterator(ITupleIterator<byte[]> tupleIterator) {
            this.tupleIterator = tupleIterator;
        }

        public boolean hasNext() {
            return tupleIterator.hasNext();
        }

        public Event next() {
            return (Event)
                    SerializerUtil.deserialize(tupleIterator.next().getValue());
        }

        public void remove() {
            tupleIterator.remove();
        }
    }


    public void insert(long startTime, Event e) {
        btree.insert(startTime, e);
    }

    public Lock writeLock() {
        return btree.writeLock();
    }

    public byte[] encodeKey(long time) {
        return tupleSer.serializeKey(time);
    }

    /**
     * Encapsulates key and value formation for the {@link EventBTree}.
     *
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class EventBTreeTupleSerializer
    extends DefaultTupleSerializer<Long, Entry> {

        /**
         *
         */
        private static final long serialVersionUID = -8429751113713375293L;

        /**
         * De-serialization ctor.
         */
        public EventBTreeTupleSerializer() {
            super();
        }

        public EventBTreeTupleSerializer(final IKeyBuilderFactory keyBuilderFactory) {
            super(keyBuilderFactory);
        }

        /**
         * Decodes the key as a timestamp.
         */
        @Override
        public Long deserializeKey(ITuple tuple) {
            final byte[] key = tuple.getKeyBuffer().array();
            final long id = KeyBuilder.decodeLong(key, 0);
            return id;
        }

        /**
         * Return the unsigned byte[] key for a timestamp.
         *
         * @param obj
         *            A timestamp.
         */
        @Override
        public byte[] serializeKey(Object obj) {
            return getKeyBuilder().reset().append((Long) obj).getKey();
        }
    }
}
