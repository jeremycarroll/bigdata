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
package com.bigdata.journal;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import org.CognitiveWeb.extser.ShortPacker;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeMetadata;
import com.bigdata.btree.IDirtyListener;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IValueSerializer;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;

/**
 * BTree mapping index names to the last metadata record committed for the named
 * index. The keys are Unicode strings using the default {@link Locale}. The
 * values are {@link Entry} objects recording the name of the index and the last
 * known address of the {@link BTreeMetadata} record for the named index.
 * <p>
 * Note: The {@link Journal} maintains an instance of this class that evolves
 * with each {@link Journal#commit()}. However, the journal also makes use of
 * historical states for the {@link Name2Addr} index in order to resolve the
 * historical state of a named index. Of necessity, the {@link Name2Addr}
 * objects used for this latter purpose MUST be distinct from the evolving
 * instance otherwise the current version of the named index would be resolved.
 * Note further that the historical {@link Name2Addr} states are accessed using
 * a canonicalizing mapping but that current evolving {@link Name2Addr} instance
 * is NOT part of that mapping.
 */
public class Name2Addr extends BTree {

    protected static final Logger log = Logger.getLogger(Name2Addr.class);

    /**
     * @todo parameterize the {@link Locale} on the Journal and pass through to
     * this class. this will make it easier to configure non-default locales.
     */
    private UnicodeKeyBuilder keyBuilder = new UnicodeKeyBuilder();

    /**
     * Cache of added/retrieved btrees by _name_. This cache is ONLY used by the
     * "live" {@link Name2Addr} instance.
     * <p>
     * Map from the name of an index to a weak reference for the corresponding
     * "live" version of the named index. Entries will be cleared from this map
     * if they are weakly reachable. In order to prevent dirty indices from
     * being cleared, we register an {@link IDirtyListener}. When it is
     * informed that an index is dirty it places a hard reference to that index
     * into the {@link #commitList}.
     * <p>
     * Note: The capacity of the backing hard reference LRU effects how many
     * _clean_ indices can be held in the cache. Dirty indices remain strongly
     * reachable owing to their existence in the {@link #commitList}.
     */
    private final WeakValueCache<String, IIndex> indexCache;

    /**
     * The capacity of the inner {@link LRUCache} for the {@link WeakValueCache}.
     * 
     * @todo make the capacity of the backing LRU a configuration option for the
     *       journal. It indirectly effects how many writable indices the
     *       journal will hold open (the effect is indirect owning to the
     *       semantics of weak references and the control of the JVM over when
     *       they are cleared). The capacity will be most important as a tuning
     *       parameter for a data service on which several hot indices are
     *       multiplexed absorbing writes. In this scenario a low capacity could
     *       starve the data service forcing frequent reloading of indices from
     *       the store rather than reuse of the existing mutable index.
     *       <p>
     *       Note: This is complicated since we re-load the {@link Name2Addr}
     *       index from the store using a fixed API unless we persist the size
     *       of the cache in the index metadata, but really you want to be able
     *       to change it each time you start the journal.
     */
    protected final int LRU_CAPACITY = 10;
    
    /**
     * Holds hard references for the dirty indices along with the index name.
     * This collection prevents dirty indices from being cleared from the
     * {@link #indexCache}, which would result in lost updates.
     * <p>
     * Note: Operations on unisolated indices always occur on the "current"
     * state of that index. The "current" state is either unchanged (following a
     * successful commit) or rolled back to the last saved state (by an abort
     * following an unsuccessful commit). Therefore all unisolated index write
     * operations MUST complete before a commit and new unisolated operations
     * MUST NOT begin until the commit has either succeeded or been rolled back.
     * Failure to observe this constraint can result in new unisolated
     * operations writing on indices that should have been rolled back if the
     * commit is not successfull.
     */
    private Set<DirtyListener> commitList = new HashSet<DirtyListener>();
    
    /**
     * An instance of this {@link DirtyListener} is registered with each named
     * index that we administer to listen for events indicating that the index
     * is dirty. When we get that event we stick the {@link DirtyListener} on
     * the {@link #commitList}. This makes the commit protocol simpler since
     * the {@link DirtyListener} has both the name of the index and the
     * reference to the index and we need both on hand to do the commit.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class DirtyListener implements IDirtyListener {
        
        final String name;
        final IIndex ndx;
        
        DirtyListener(String name,IIndex ndx) {
            
            assert name!=null;
            
            assert ndx!=null;
            
            this.name = name;
            
            this.ndx = ndx;
            
        }

        /**
         * Add <i>this</i> to the {@link Name2Addr#commitList}.
         *  
         * @param btree
         */
        public void dirtyEvent(BTree btree) {

            assert btree == this.ndx;
            
            log.info("Adding dirty index to commit list: ndx="+name);
            
            commitList.add(this);
            
        }

    }
    
    public Name2Addr(IRawStore store) {

        super(store, DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                new ValueSerializer());

        // Note: code shared by both constructors.
        indexCache = new WeakValueCache<String, IIndex>(new LRUCache<String, IIndex>(LRU_CAPACITY));

    }

    /**
     * Load from the store (de-serialization constructor).
     * 
     * @param store
     *            The backing store.
     * @param metadataId
     *            The metadata record for the index.
     */
    public Name2Addr(IRawStore store, BTreeMetadata metadata) {

        super(store, metadata);
        
        // Note: code shared by both constructors.
        indexCache = new WeakValueCache<String, IIndex>(new LRUCache<String, IIndex>(LRU_CAPACITY));

    }

    /**
     * True iff the index is on the commit list.
     * 
     * @param btree
     * 
     * @return
     */
    boolean willCommit(IIndex btree) {
    
        return commitList.contains(btree);
        
    }
    
    /**
     * Extends the default behavior to cause each named btree to flush itself to
     * the store, updates the address from which that btree may be reloaded
     * within its internal mapping, and finally flushes itself and returns the
     * address from which this btree may be reloaded.
     */
    public long handleCommit() {

        // visit the indices on the commit list.
        Iterator<DirtyListener> itr = commitList.iterator();
        
        while(itr.hasNext()) {
            
            DirtyListener l = itr.next();
            
            String name = l.name;
            
            IIndex btree = l.ndx;
            
            log.info("Will commit: "+name);
            
            // request commit.
            long addr = ((ICommitter)btree).handleCommit();
            
            // encode the index name as a key.
            final byte[] key = getKey(name);
            
            // lookup the current entry (if any) for that index.
            Entry oldValue = (Entry) lookup(key);
            
            // if there is no existing entry or if the addr has changed.
            if (oldValue == null || oldValue.addr != addr) {

                // then update persistent mapping.
                insert(key, new Entry(name, addr));
                
            }
            
//            // place into the object cache on the journal.
//            journal.touch(addr, btree);
            
        }

        // Clear the commit list.
        commitList.clear();
        
        // and flushes out this btree as well.
        return super.handleCommit();
        
    }
    
    /**
     * Encodes a unicode string into a key.
     * 
     * @param name
     *            The name of the btree.
     *            
     * @return The corresponding key.
     */
    protected byte[] getKey(String name) {

        return keyBuilder.reset().append(name).getKey();

    }

    /**
     * Return the named index - this method tests a cache of the named btrees
     * and will return the same instance if the index is found in the cache.
     * 
     * @param name
     *            The index name.
     * 
     * @return The named index or <code>null</code> iff there is no index with
     *         that name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>.
     */
    public IIndex get(String name) {

        if (name == null)
            throw new IllegalArgumentException();
        
        IIndex btree = indexCache.get(name);
        
        if (btree != null)
            return btree;

        final Entry entry = (Entry) super.lookup(getKey(name));

        if (entry == null) {

            return null;
            
        }

//        /*
//         * Reload the index from the store using the object cache to ensure a
//         * canonicalizing mapping.
//         */
//        btree = journal.getIndex(entry.addr);
        
        // re-load btree from the store.
        btree = BTree.load(this.store, entry.addr);
        
        // save name -> btree mapping in transient cache.
//        indexCache.put(name,btree);
        indexCache.put(name, btree, false/*dirty*/);

        // listen for dirty events so that we know when to add this to the commit list.
        ((BTree)btree).setDirtyListener(new DirtyListener(name,btree));
        
        // report event (loaded btree).
        ResourceManager.openUnisolatedBTree(name);

        // return btree.
        return btree;

    }
    
    /**
     * Return the address from which the historical state of the named index may
     * be loaded.
     * <p>
     * Note: This is a lower-level access mechanism that is used by
     * {@link Journal#getIndex(String, ICommitRecord)} when accessing historical
     * named indices from an {@link ICommitRecord}.
     * 
     * @param name
     *            The index name.
     * 
     * @return The address or <code>0L</code> if the named index was not
     *         registered.
     */
    protected long getAddr(String name) {

        /*
         * Note: This uses a private cache to reduce the Unicode -> key
         * translation burden. We can not use the normal cache since that maps
         * the name to the index and we have to return the address not the index
         * in order to support a canonicalizing mapping in the Journal.
         */
        synchronized (addrCache) {

            Long addr = addrCache.get(name);

            if (addr == null) {

                final Entry entry = (Entry) super.lookup(getKey(name));

                if (entry == null) {

                    addr = 0L;
                    
                } else {
                    
                    addr = entry.addr;
                    
                }

                addrCache.put(name, addr);
                
            }

            return addr;

        }

    }
    /**
     * A private cache used only by {@link #getAddr(String)}.
     */
    private HashMap<String/* name */, Long/* Addr */> addrCache = new HashMap<String, Long>();

    /**
     * Add an entry for the named index.
     * 
     * @param name
     *            The index name.
     * 
     * @param btree
     *            The index.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if <i>btree</i> is <code>null</code>.
     * @exception IndexExistsException
     *                if there is already an index registered under that name.
     */
    public void registerIndex(String name,IIndex btree) {
        
        if (name == null)
            throw new IllegalArgumentException();

        if (btree == null)
            throw new IllegalArgumentException();
        
        if( ! (btree instanceof ICommitter) ) {
            
            throw new IllegalArgumentException("Index does not implement: "
                    + ICommitter.class);
            
        }

        final byte[] key = getKey(name);
        
        if(super.contains(key)) {
            
            throw new IndexExistsException(name);
            
        }
        
        // flush btree to the store to get the metadata record address.
        final long addr = ((ICommitter)btree).handleCommit();
        
        // add an entry to the persistent index.
        super.insert(key,new Entry(name,addr));
        
//        // touch the btree in the journal's object cache.
//        journal.touch(addr, btree);
        
        // add name -> btree mapping to the transient cache.
        indexCache.put(name, btree, true/*dirty*/);
        
        DirtyListener l = new DirtyListener(name,btree);
        
        // add to the commit list.
        commitList.add( l );

        // set listener on the btree as well.
        ((BTree)btree).setDirtyListener( l );
        
    }

    /**
     * Removes the entry for the named index. The named index will no longer
     * participate in commits.
     * 
     * @param name
     *            The index name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>.
     * @exception NoSuchIndexException
     *                if the index does not exist.
     */
    public void dropIndex(String name) {

        if (name == null)
            throw new IllegalArgumentException();

        final byte[] key = getKey(name);
        
        if(!super.contains(key)) {
            
            throw new NoSuchIndexException("Not registered: "+name);
            
        }
        
        // remove the name -> btree mapping from the transient cache.
        IIndex btree = indexCache.remove(name);
        
        if (btree != null) {

            /*
             * Make sure that the index is not on the commit list.
             * 
             * Note: If the index is not in the index cache then it WILL NOT be
             * in the commit list.
             */
            
            commitList.remove(btree);
            
            // clear our listener.
            ((BTree)btree).setDirtyListener(null);

        }

        /*
         * Remove the entry from the persistent index. After a commit you will
         * no longer be able to find the metadata record for this index from the
         * current commit record (it will still exist of course in historical
         * commit records).
         */
        super.remove(key);

    }
    
    /**
     * An entry in the persistent index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Entry {
       
        /**
         * The name of the index.
         */
        public final String name;
        
        /**
         * The address of the last known {@link BTreeMetadata} record for the
         * index with that name.
         */
        public final long addr;
        
        public Entry(String name,long addr) {
            
            this.name = name;
            
            this.addr = addr;
            
        }
        
    }
    
    /**
     * The values are {@link Entry}s.
     *
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ValueSerializer implements IValueSerializer {

        private static final long serialVersionUID = 6428956857269979589L;

//        public static transient final IValueSerializer INSTANCE = new ValueSerializer();

        final public static transient short VERSION0 = 0x0;

        // FIXME This is not parameterized by the actual offsetBits. 
//        final protected transient IRawStore store;
        final private static transient IAddressManager addressManager = WormAddressManager.INSTANCE;

        public ValueSerializer() {
            
        }
        
//        public ValueSerializer(IRawStore store) {
//            
//            if(store==null) throw new IllegalArgumentException();
//            
//            this.store = store;
//            
//        }

        public void putValues(DataOutputBuffer os, Object[] values, int n)
                throws IOException {

            os.packShort(VERSION0);

            for (int i = 0; i < n; i++) {

                Entry entry = (Entry) values[i];

                os.writeUTF(entry.name);

                addressManager.packAddr(os, entry.addr);

//                if (packedLongs) {
//
//                    LongPacker.packLong(os, entry.addr);
//
//                } else {
//
//                    os.writeLong(entry.addr);
//
//                }
                
            }

        }
        
        public void getValues(DataInput is, Object[] values, int n)
                throws IOException {

            final short version = ShortPacker.unpackShort(is);
            
            if (version != VERSION0)
                throw new RuntimeException("Unknown version: " + version);

            for (int i = 0; i < n; i++) {

                final String name = is.readUTF();
                
                final long addr;
                
//                if (packedLongs) {
//
//                    addr = Long.valueOf(LongPacker.unpackLong(is));
//
//                } else {
//
//                    addr = Long.valueOf(is.readLong());
//
//                }
                
                addr = addressManager.unpackAddr(is);
                
                values[i] = new Entry(name,addr);

            }

        }

    }

}
