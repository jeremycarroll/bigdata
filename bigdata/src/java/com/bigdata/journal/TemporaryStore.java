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
/*
 * Created on Feb 21, 2007
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ByteArrayValueSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.rawstore.WormAddressManager;

/**
 * A temporary store that supports named indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TemporaryStore extends TemporaryRawStore implements IIndexManager {

    /**
     * 
     */
    public TemporaryStore() {

        this(WormAddressManager.DEFAULT_OFFSET_BITS,
                DEFAULT_INITIAL_IN_MEMORY_EXTENT,
                DEFAULT_MAXIMUM_IN_MEMORY_EXTENT, false);
        
    }

    /**
     * @param offsetBits
     *            This determines the capacity of the store file and the maximum
     *            length of a record.  The value is passed through to
     *            {@link WormAddressManager#WormAddressManager(int)}.
     * @param initialInMemoryExtent
     * @param maximumInMemoryExtent
     * @param useDirectBuffers
     */
    public TemporaryStore(int offsetBits, long initialInMemoryExtent,
            long maximumInMemoryExtent, boolean useDirectBuffers) {

        super(offsetBits, initialInMemoryExtent, maximumInMemoryExtent,
                useDirectBuffers);

        setupName2AddrBTree();

    }
    
    /**
     * BTree mapping index names to the last metadata record committed for the
     * named index. The keys are index names (unicode strings). The values are
     * the last known address of the named btree.
     * <p>
     * Note: This is a mutable {@link BTree} so it is NOT thread-safe. We always
     * synchronize on this object before accessing it.
     */
    private Name2Addr name2Addr;

    /**
     * Setup the btree that resolved named btrees.
     */
    private void setupName2AddrBTree() {

        assert name2Addr == null;
        
        name2Addr = new Name2Addr(this);

    }

    /**
     * Registers a {@link BTree} whose values are variable length byte[]s.
     */
    public IIndex registerIndex(String name) {
    
        return registerIndex(name, new BTree(this,
                BTree.DEFAULT_BRANCHING_FACTOR,
                UUID.randomUUID(),
                ByteArrayValueSerializer.INSTANCE));
        
    }
    
    public IIndex registerIndex(String name, IIndex btree) {

        synchronized (name2Addr) {

            // add to the persistent name map.
            name2Addr.registerIndex(name, btree);

            return btree;

        }
        
    }
    
    public void dropIndex(String name) {
        
        synchronized(name2Addr) {

            // drop from the persistent name map.
            name2Addr.dropIndex(name);
            
        }
        
    }

    /**
     * Return the named index (unisolated). Writes on the returned index will be
     * made restart-safe with the next {@link #commit()} regardless of the
     * success or failure of a transaction. Transactional writes must use the
     * same named method on the {@link Tx} in order to obtain an isolated
     * version of the named btree.
     */
    public IIndex getIndex(String name) {

        synchronized(name2Addr) {

            return name2Addr.get(name);
            
        }

    }

}
