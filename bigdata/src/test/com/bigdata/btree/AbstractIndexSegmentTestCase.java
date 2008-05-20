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
 * Created on May 20, 2008
 */

package com.bigdata.btree;

import com.bigdata.btree.IndexSegment.LeafIterator;
import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableLeaf;

/**
 * Adds some methods for testing an {@link IndexSegment} for consistency.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractIndexSegmentTestCase extends AbstractBTreeTestCase {

    /**
     * 
     */
    public AbstractIndexSegmentTestCase() {
    }

    /**
     * @param name
     */
    public AbstractIndexSegmentTestCase(String name) {
        super(name);
    }

    /**
     * Test forward leaf scan.
     */
    public void testForwardScan(IndexSegment seg)
    {

        final ImmutableLeaf firstLeaf = seg.readLeaf(seg.getStore().getCheckpoint().addrFirstLeaf);
        assertEquals("priorAddr", 0L, firstLeaf.priorAddr);

        final ImmutableLeaf lastLeaf = seg.readLeaf(seg.getStore().getCheckpoint().addrLastLeaf);
        assertEquals("nextAddr", 0L, lastLeaf.nextAddr);

        final LeafIterator itr = seg.leafIterator(true/*forwardScan*/);
        
        assertTrue(firstLeaf==itr.current()); // Note: test depends on cache!
        
        ImmutableLeaf priorLeaf = itr.current();
        
        for(int i=1; i<seg.getLeafCount(); i++) {
            
            ImmutableLeaf current = itr.next();
            
            assertEquals("priorAddr",priorLeaf.getIdentity(),current.priorAddr);

            priorLeaf = current;

            if (current == lastLeaf) {

                // last leaf.
                assertFalse(itr.hasNext());
                
            } else {
                
                // should be more.
                assertTrue(itr.hasNext());
                
            }
            
        }
        
    }
    
    /**
     * Test reverse leaf scan.
     * 
     * Note: the scan starts with the last leaf in the key order and then
     * proceeds in reverse key order.
     */
    public void testReverseScan(IndexSegment seg) {
        
        final ImmutableLeaf firstLeaf = seg.readLeaf(seg.getStore().getCheckpoint().addrFirstLeaf);
        assertEquals("priorAddr", 0L, firstLeaf.priorAddr);

        final ImmutableLeaf lastLeaf = seg.readLeaf(seg.getStore().getCheckpoint().addrLastLeaf);
        assertEquals("nextAddr", 0L, lastLeaf.nextAddr);

        final LeafIterator itr = seg.leafIterator(false/*forwardScan*/);
        
        assertTrue(lastLeaf==itr.current()); // Note: test depends on cache!
        
        ImmutableLeaf nextLeaf = itr.current();
        
        for(int i=1; i<seg.getLeafCount(); i++) {
            
            ImmutableLeaf current = itr.next();
            
            assertEquals("nextAddr",nextLeaf.getIdentity(),current.nextAddr);

            nextLeaf = current;

            if (current == firstLeaf) {

                // last leaf.
                assertFalse(itr.hasNext());
                
            } else {
                
                // should be more.
                assertTrue(itr.hasNext());
                
            }
            
        }
        
    }

}
