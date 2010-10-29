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
 * Created on Apr 23, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.ndx.ClientIndexView;

//BTM
import com.bigdata.util.Util;

/**
 * Test suite for the {@link EmbeddedClient}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEmbeddedClient extends AbstractEmbeddedFederationTestCase {

//BTM
private String dbgFlnm = (TestEmbeddedClient.class).getSimpleName()+".txt";

    public TestEmbeddedClient() {
//BTM
Util.printStr(dbgFlnm, "TestEmbeddedClient: constructor-1");
    }

    public TestEmbeddedClient(String name) {
        super(name);
Util.printStr(dbgFlnm, "TestEmbeddedClient: constructor-2");
    }

    /**
     * Test ability to register a scale-out index, access it, and then drop the
     * index.
     */
    public void test_registerIndex() {

Util.printStr(dbgFlnm, "TestEmbeddedClient: test_registerIndex - ENTER");
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());
        
        metadata.setDeleteMarkers(true);

        // verify index does not exist.
        assertNull(fed.getIndex(name,ITx.UNISOLATED));
        assertNull(fed.getIndex(name,ITx.READ_COMMITTED));
        
        // register.
        fed.registerIndex(metadata);
        
        // obtain unisolated view.
        {

            final long tx = ITx.UNISOLATED;

            final IIndex ndx = fed.getIndex(name, tx);

            // verify view is non-null
            assertNotNull(ndx);

            // verify same index UUID.
            assertEquals(metadata.getIndexUUID(), ndx.getIndexMetadata()
                    .getIndexUUID());
            
        }
        
        // obtain read-committed view.
        {

            final long tx = ITx.READ_COMMITTED;

            final IIndex ndx = fed.getIndex(name, tx);

            // verify view is non-null
            assertNotNull(ndx);

            // verify same index UUID.
            assertEquals(metadata.getIndexUUID(), ndx.getIndexMetadata()
                    .getIndexUUID());

        }
        
        // drop the index.
        fed.dropIndex(name);
        
        // no longer available to read committed requests.
        assertNull(fed.getIndex(name, ITx.READ_COMMITTED));
        
        // no longer available to unisolated requests.
        assertNull(fed.getIndex(name, ITx.UNISOLATED));
        
        /*
         * @todo obtain a valid commit timestamp for the index during the period
         * in which it existed and verify that a historical view may still be
         * obtained for that timestamp.
         */
Util.printStr(dbgFlnm, "TestEmbeddedClient: test_registerIndex - EXIT");

    }

    /**
     * Tests the ability to statically partition a scale-out index.
     */
    public void XXXtest_staticPartitioning() throws Exception {
Util.printStr(dbgFlnm, "TestEmbeddedClient: test_staticPartitioning - ENTER");
        
        final String name = "testIndex";
        
        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        metadata.setDeleteMarkers(true);
//BTM
UUID dataService0UUID = null;
if(dataService0 instanceof IService) {
    dataService0UUID = ((IService)dataService0).getServiceUUID();
} else {
    dataService0UUID = ((Service)dataService0).getServiceUUID();
}
UUID dataService1UUID = null;
if(dataService1 instanceof IService) {
    dataService1UUID = ((IService)dataService1).getServiceUUID();
} else {
    dataService1UUID = ((Service)dataService1).getServiceUUID();
}

        UUID indexUUID = fed.registerIndex(metadata, new byte[][]{//
                new byte[]{},
                new byte[]{5}
        }, new UUID[]{//
//BTM                dataService0.getServiceUUID(),
//BTM                dataService1.getServiceUUID()
dataService0UUID,
dataService1UUID
        });

        final int partitionId0 = 0;
        final int partitionId1 = 1;
        
        /*
         * Verify index is registered on each data service under the correct
         * name for that index partition. The index on each data service must
         * have the same indexUUID since they are just components of the same
         * scale-out index.
         */
//BTM        assertIndexRegistered(dataService0, DataService.getIndexPartitionName(name, partitionId0), indexUUID);
//BTM        assertIndexRegistered(dataService1, DataService.getIndexPartitionName(name, partitionId1), indexUUID);
assertIndexRegistered(dataService0, Util.getIndexPartitionName(name, partitionId0), indexUUID);
assertIndexRegistered(dataService1, Util.getIndexPartitionName(name, partitionId1), indexUUID);

        /*
         * Verify metadata for index partition#0 on dataService0
         * 
         * @todo test more of the metadata for correctness.
         */
        {

//BTM            IndexMetadata actual = dataService0.getIndexMetadata(DataService.getIndexPartitionName(name, partitionId0), ITx.UNISOLATED);
IndexMetadata actual = ((ShardManagement)dataService0).getIndexMetadata(Util.getIndexPartitionName(name, partitionId0), ITx.UNISOLATED);

            // verify index partition exists on that data service.
            assertNotNull(actual);

            // partition metadata.
            assertEquals("partitionId", partitionId0, actual
                    .getPartitionMetadata().getPartitionId());

            assertEquals("leftSeparator", new byte[] {}, actual
                    .getPartitionMetadata().getLeftSeparatorKey());
            
            assertEquals("rightSeparator", new byte[] { 5 }, actual
                    .getPartitionMetadata().getRightSeparatorKey());
            
            // other metadata.
            assertEquals(metadata.getDeleteMarkers(),actual.getDeleteMarkers());
            
        }

        /*
         * Verify metadata for index partition#1 on dataService1
         */
        {

//BTM            IndexMetadata actual = dataService1.getIndexMetadata(DataService.getIndexPartitionName(name, partitionId1), ITx.UNISOLATED);
IndexMetadata actual = ((ShardManagement)dataService1).getIndexMetadata(Util.getIndexPartitionName(name, partitionId1), ITx.UNISOLATED);

            // verify index partition exists on that data service.
            assertNotNull(actual);
            
            // partition metadata
            assertEquals("partitionId", partitionId1, actual
                    .getPartitionMetadata().getPartitionId());

            assertEquals("leftSeparator", new byte[] {5}, actual
                    .getPartitionMetadata().getLeftSeparatorKey());
            
            assertEquals("rightSeparator", null, actual.getPartitionMetadata()
                    .getRightSeparatorKey());

            // other metadata
            assertEquals(metadata.getDeleteMarkers(),actual.getDeleteMarkers());
            
        }
Util.printStr(dbgFlnm, "TestEmbeddedClient: test_staticPartitioning - EXIT");
        
    }
    
    /**
     * Test of the routine responsible for identifying the split points in an
     * ordered set of keys for a batch index operation. Note that the routine
     * requires access to the partition definitions in the form of a
     * {@link MetadataIndex} in order to identify the split points in the
     * keys[].
     */
    public void XXXtest_splitKeys_staticPartitions01() throws IOException {
Util.printStr(dbgFlnm, "TestEmbeddedClient: test_splitKeys_staticPartitions01 - ENTER");
        
        final String name = "testIndex";

Util.printStr(dbgFlnm, "    new IndexMetadata");
        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        metadata.setDeleteMarkers(true);
//BTM
UUID dataService0UUID = null;
if(dataService0 instanceof IService) {
    dataService0UUID = ((IService)dataService0).getServiceUUID();
} else {
    dataService0UUID = ((Service)dataService0).getServiceUUID();
}
UUID dataService1UUID = null;
if(dataService1 instanceof IService) {
    dataService1UUID = ((IService)dataService1).getServiceUUID();
} else {
    dataService1UUID = ((Service)dataService1).getServiceUUID();
}
Util.printStr(dbgFlnm, "    dataService0UUID = "+dataService0UUID);
Util.printStr(dbgFlnm, "    dataService1UUID = "+dataService1UUID);
        /*
         * Register and statically partition an index.
         */
Util.printStr(dbgFlnm, "    fed.registerIndex");
        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{}, // keys less than 5...
                new byte[]{5} // keys GTE 5....
        }, new UUID[]{//
//BTM                dataService0.getServiceUUID(),
//BTM                dataService1.getServiceUUID()
dataService0UUID,
dataService1UUID
        });
        
        /*
         * Request a view of that index.
         */
Util.printStr(dbgFlnm, "    ndx = fed.getIndex(name="+name+")");
        ClientIndexView ndx = (ClientIndexView) fed.getIndex(name,ITx.UNISOLATED);

        /*
         * Range count the index to verify that it is empty.
         */

Util.printStr(dbgFlnm, "    ndx.rangeCount(null, null) =");
Util.printStr(dbgFlnm, "                                 "+ndx.rangeCount(null, null));
Util.printStr(dbgFlnm, "    assertEquals - rangeCount");
        assertEquals("rangeCount",0,ndx.rangeCount(null, null));

        /*
         * Get metadata for the index partitions that we will need to verify
         * the splits.
         */
Util.printStr(dbgFlnm, "    pmd0 = ndx.getMetadataIndex");
        final PartitionLocator pmd0 = ndx.getMetadataIndex().get(new byte[]{});
Util.printStr(dbgFlnm, "    pmd1 = ndx.getMetadataIndex");
        final PartitionLocator pmd1 = ndx.getMetadataIndex().get(new byte[]{5});
Util.printStr(dbgFlnm, "    assertNotNull pmd0");
        assertNotNull("partition#0",pmd0);
Util.printStr(dbgFlnm, "    assertNotNull pmd1");
        assertNotNull("partition#1",pmd1);
        
        /*
         * Setup data and test splitKeys().
         * 
         * Note: In this test there is a key that is an exact match on the 
         * separator key between the index partitions.
         */
        {
            
            final byte[][] keys = new byte[][] {//
            new byte[]{1}, // [0]
            new byte[]{2}, // [1]
            new byte[]{5}, // [2]
            new byte[]{6}, // [3]
            new byte[]{9}  // [4]
            };
            
Util.printStr(dbgFlnm, "    splits = ndx.splitKeys - 0");
            final List<Split> splits = ndx.splitKeys(ITx.UNISOLATED, 0, keys.length,
                    keys);
        
Util.printStr(dbgFlnm, "    assertNotNull splits - 0");
            assertNotNull(splits);
            
Util.printStr(dbgFlnm, "    assertEquals # of splits = 2 [0]");
            assertEquals("#splits", 2, splits.size());

Util.printStr(dbgFlnm, "    assertEquals new Split pmd0 - 0");
            assertEquals(new Split(pmd0, 0, 2), splits.get(0));
Util.printStr(dbgFlnm, "    assertEquals new Split pmd1 - 0");
            assertEquals(new Split(pmd1, 2, 5), splits.get(1));
            
        }

        /*
         * Variant in which there are duplicates of the key that corresponds to
         * the rightSeparator for the 1st index partition. This causes a problem
         * where the binarySearch returns the index of ONE of the keys that is
         * equal to the rightSeparator key and we need to back up until we have
         * found the FIRST ONE. While not every example with duplicate keys
         * equal to the rightSeparator will trigger the problem, this example
         * will.
         */
        {
            
            final byte[][] keys = new byte[][] {//
            new byte[]{1}, // [0]
            new byte[]{5}, // [1]
            new byte[]{5}, // [2]
            new byte[]{5}, // [3]
            new byte[]{9}  // [4]
            };
            
Util.printStr(dbgFlnm, "    splits = ndx.splitKeys - 1");
            final List<Split> splits = ndx.splitKeys(ITx.UNISOLATED, 0,
                    keys.length, keys);
        
Util.printStr(dbgFlnm, "    assertNotNull splits - 1");
            assertNotNull(splits);
            
Util.printStr(dbgFlnm, "    assertEquals # of splits = 2 [1]");
            assertEquals("#splits", 2, splits.size());

Util.printStr(dbgFlnm, "    assertEquals new Split pmd0 - 1");
            assertEquals(new Split(pmd0, 0, 1), splits.get(0));
Util.printStr(dbgFlnm, "    assertEquals new Split pmd1 - 1");
            assertEquals(new Split(pmd1, 1, 5), splits.get(1));
            
        }

        /*
         * Setup data and test splitKeys().
         * 
         * Note: In this test there is NOT an exact match on the separator key
         * between the index partitions. This will result in a negative encoding
         * of the insertion point by the binary search routine. This test
         * verifies that the correct index is selected for the last key to enter
         * the first partition.
         */
        {
            
            final byte[][] keys = new byte[][] {//
            new byte[]{1}, // [0]
            new byte[]{2}, // [1]
            new byte[]{4}, // [2]
            new byte[]{6}, // [3]
            new byte[]{9}  // [4]
            };
            
Util.printStr(dbgFlnm, "    splits = ndx.splitKeys - 2");
            final List<Split> splits = ndx.splitKeys(ITx.UNISOLATED, 0,
                    keys.length, keys);
        
Util.printStr(dbgFlnm, "    assertNotNull splits - 2");
            assertNotNull(splits);

Util.printStr(dbgFlnm, "    assertEquals # of splits = 2 [2]");
            assertEquals("#splits", 2, splits.size());

Util.printStr(dbgFlnm, "    assertEquals new Split pmd0 - 2");
            assertEquals(new Split(pmd0, 0, 3), splits.get(0));
Util.printStr(dbgFlnm, "    assertEquals new Split pmd1 - 2");
            assertEquals(new Split(pmd1, 3, 5), splits.get(1));
            
        }
Util.printStr(dbgFlnm, "TestEmbeddedClient: test_splitKeys_staticPartitions01 - EXIT");
                
    }

    public void XXXtest_addDropIndex_twoPartitions() throws IOException {
Util.printStr(dbgFlnm, "TestEmbeddedClient: test_addDropIndex_twoPartitions - ENTER");
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        metadata.setDeleteMarkers(true);
//BTM
UUID dataService0UUID = null;
if(dataService0 instanceof IService) {
    dataService0UUID = ((IService)dataService0).getServiceUUID();
} else {
    dataService0UUID = ((Service)dataService0).getServiceUUID();
}
UUID dataService1UUID = null;
if(dataService1 instanceof IService) {
    dataService1UUID = ((IService)dataService1).getServiceUUID();
} else {
    dataService1UUID = ((Service)dataService1).getServiceUUID();
}
        /*
         * Register and statically partition an index.
         */
        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{}, // keys less than 5...
                new byte[]{5} // keys GTE 5....
        }, new UUID[]{//
//BTM                dataService0.getServiceUUID(),
//BTM                dataService1.getServiceUUID()
dataService0UUID,
dataService1UUID
        });

        // view of that index.
        final IIndex ndx = fed.getIndex(name, ITx.UNISOLATED);
        
        assertNotNull("Expecting index to be registered", ndx);
        
        /*
         * Range count the index to verify that it is empty.
         */
        assertEquals("rangeCount", 0, ndx.rangeCount());
        
        // drop the index.
        fed.dropIndex(name);

        // request view of index.
        assertNull("Not expecting index to exist",
                fed.getIndex(name,ITx.UNISOLATED));
        
Util.printStr(dbgFlnm, "TestEmbeddedClient: test_addDropIndex_twoPartitions - EXIT");
    }
    
}
