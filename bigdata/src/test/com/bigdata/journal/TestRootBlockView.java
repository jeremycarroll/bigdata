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
 * Created on Oct 18, 2006
 */

package com.bigdata.journal;

import java.util.Random;

import com.bigdata.util.TimestampFactory;

import junit.framework.TestCase2;

/**
 * Test suite for {@link RootBlockView}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRootBlockView extends TestCase2 {

    /**
     * 
     */
    public TestRootBlockView() {
    }

    /**
     * @param arg0
     */
    public TestRootBlockView(String arg0) {
        super(arg0);
    }

    /**
     * Constructor correct acceptance stress test.
     */
    public void test_ctor() {
        
        final Random r = new Random();
        
        final int limit = 10000;
        
        for (int i = 0; i < limit; i++) {

            final boolean rootBlock0 = r.nextBoolean();
            final int segmentId = r.nextInt();
            final boolean anyTransactions = r.nextInt(100)>90;
            final long firstTxId = anyTransactions?TimestampFactory.nextNanoTime():0L;
            final long lastTxId = anyTransactions?TimestampFactory.nextNanoTime():0L;
            final int nextOffset = r.nextInt(Integer.MAX_VALUE);
            final long commitCounter = r.nextInt(Integer.MAX_VALUE);
            final long[] rootAddrs = new long[RootBlockView.MAX_ROOT_ADDRS];
            for( int j=0; j<rootAddrs.length; j++ ) {
                rootAddrs[j] = r.nextLong();
            }

            RootBlockView rootBlock = new RootBlockView(rootBlock0, segmentId,
                    nextOffset, firstTxId, lastTxId, commitCounter, rootAddrs);

            System.err.println("pass=" + i + " of " + limit + " : timestamp="
                    + rootBlock.getTimestamp());

            // Verify the view.
            rootBlock.valid();
            assertEquals("rootBlock0", rootBlock0, rootBlock.isRootBlock0());
            assertEquals("segmentId", segmentId, rootBlock.getSegmentId());
            assertEquals("nextOffset", nextOffset, rootBlock.getNextOffset());
            assertEquals("firstTxId", firstTxId, rootBlock.getFirstTxId());
            assertEquals("lastTxId", lastTxId, rootBlock.getLastTxId());
            assertEquals("rootIds", rootAddrs, rootBlock.getRootAddrs());
            for( int j=0; j<rootAddrs.length; j++ ) {
                assertEquals("addrRoot[" + j + "]", rootAddrs[j], rootBlock
                        .getRootAddr(j));
            }
            assertEquals("commitCounter", commitCounter, rootBlock
                    .getCommitCounter());

            // create a view from the backing byte buffer.
            rootBlock = new RootBlockView(rootBlock0,rootBlock.asReadOnlyBuffer());
            
            // Verify the view.
            rootBlock.valid();
            assertEquals("rootBlock0", rootBlock0, rootBlock.isRootBlock0());
            assertEquals("segmentId", segmentId, rootBlock.getSegmentId());
            assertEquals("nextOffset", nextOffset, rootBlock.getNextOffset());
            assertEquals("firstTxId", firstTxId, rootBlock.getFirstTxId());
            assertEquals("lastTxId", lastTxId, rootBlock.getLastTxId());
            for( int j=0; j<rootAddrs.length; j++ ) {
                assertEquals("addrRoot[" + j + "]", rootAddrs[j], rootBlock
                        .getRootAddr(j));
            }
            assertEquals("commitCounter", commitCounter, rootBlock
                    .getCommitCounter());

        }
        
    }

    /**
     * Correct rejection tests for the constructor.
     */
    public void test_ctor_correctRejection() {

        System.err.println("sizeof(RootBlock): "+RootBlockView.SIZEOF_ROOT_BLOCK);
        
        final boolean rootBlock0 = true; // all values are legal.
        final int segmentId = 0; // no constraint
        final int nextOffsetOk = 100;
        final int nextOffsetBad = -1;
        // note: choose tx timestamps in named pairs for tests.
        final long firstTxIdOk = 0L;
        final long lastTxIdOk = 0L;
        final long firstTxIdOk2 = TimestampFactory.nextNanoTime();
        final long lastTxIdOk2 = TimestampFactory.nextNanoTime();
        final long firstTxIdBad1 = TimestampFactory.nextNanoTime();
        final long lastTxIdBad1 = 0L;
        final long firstTxIdBad2 = 0L;
        final long lastTxIdBad2 = TimestampFactory.nextNanoTime();
        final long lastTxIdBad3 = TimestampFactory.nextNanoTime(); // note: out of order.
        final long firstTxIdBad3 = TimestampFactory.nextNanoTime(); // note: out of order.
        final long[] rootIdsOk = new long[RootBlockView.MAX_ROOT_ADDRS];
        final long[] rootIdsBad = null; // null.
        final long[] rootIdsBad2 = new long[RootBlockView.MAX_ROOT_ADDRS-1]; // too small.
        final long[] rootIdsBad3 = new long[RootBlockView.MAX_ROOT_ADDRS+1]; // too large.
        final long commitCounterOk = 0;
        final long commitCounterBad = -1; // negative
        final long commitCounterBad2 = Long.MAX_VALUE; // too large.
        
        // legit.
        new RootBlockView(rootBlock0, segmentId, nextOffsetOk, firstTxIdOk,
                lastTxIdOk, commitCounterOk, rootIdsOk);
        // legit.
        new RootBlockView(rootBlock0, segmentId, nextOffsetOk, firstTxIdOk2,
                lastTxIdOk2, commitCounterOk, rootIdsOk);

        // bad next offset
        try {
            new RootBlockView(rootBlock0, segmentId, nextOffsetBad,
                    firstTxIdOk, lastTxIdOk, commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        
        // bad transaction first,last timestamps.
        try {
            new RootBlockView(rootBlock0, segmentId, nextOffsetOk,
                    firstTxIdBad1, lastTxIdBad1, commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, nextOffsetOk,
                    firstTxIdBad2, lastTxIdBad2, commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, nextOffsetOk,
                    firstTxIdBad3, lastTxIdBad3, commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad commit counter
        try {
            new RootBlockView(rootBlock0, segmentId, nextOffsetOk, firstTxIdOk,
                    lastTxIdOk, commitCounterBad, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, nextOffsetOk, firstTxIdOk,
                    lastTxIdOk, commitCounterBad2, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad root ids.
        // @todo try with bad addresses.
        try {
            new RootBlockView(rootBlock0, segmentId, nextOffsetOk, firstTxIdOk,
                    lastTxIdOk, commitCounterOk, rootIdsBad);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, nextOffsetOk, firstTxIdOk,
                    lastTxIdOk, commitCounterOk, rootIdsBad2);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, nextOffsetOk, firstTxIdOk,
                    lastTxIdOk, commitCounterOk, rootIdsBad3);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

    }

}
