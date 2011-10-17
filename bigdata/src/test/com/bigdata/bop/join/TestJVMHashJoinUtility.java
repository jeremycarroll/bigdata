/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Oct 14, 2011
 */

package com.bigdata.bop.join;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.join.JVMHashJoinUtility.Bucket;
import com.bigdata.bop.join.JVMHashJoinUtility.Key;
import com.bigdata.striterator.Chunkerator;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Test suite for the {@link JVMHashJoinUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestJVMHashJoinUtility extends AbstractHashJoinUtilityTestCase {

    /**
     * 
     */
    public TestJVMHashJoinUtility() {
    }

    /**
     * @param name
     */
    public TestJVMHashJoinUtility(String name) {
        super(name);
    }
    
    private Map<Key,Bucket> rightSolutions;

    @Override
    protected void tearDown() throws Exception {

        if (rightSolutions != null) {
            rightSolutions = null;
        }

        super.tearDown();

    }

    @Override
    protected void setUp() throws Exception {

        super.setUp();
    
        rightSolutions = new LinkedHashMap<Key, Bucket>();

    }

    /**
     * Test helper.
     * 
     * @param optional
     * @param joinVars
     * @param selectVars
     * @param left
     * @param right
     * @param expected
     */
    protected void doHashJoinTest(//
            final boolean optional,//
            final IVariable<?>[] joinVars,//
            final IVariable<?>[] selectVars,//
            final IConstraint[] constraints,//
            final List<IBindingSet> left, //
            final List<IBindingSet> right,//
            final IBindingSet[] expected//
            ) {

        // Load the right solutions into the HTree.
        {
        
            final BOpStats stats = new BOpStats();
            
            JVMHashJoinUtility.acceptSolutions(
                    new Chunkerator<IBindingSet>(right.iterator()), joinVars, stats,
                    rightSolutions, optional);

//            assertEquals(right.size(), rightSolutions.size();getEntryCount());

            assertEquals(right.size(), stats.unitsIn.get());

        }

        /*
         * Run the hash join.
         */

        final ICloseableIterator<IBindingSet> leftItr = new CloseableIteratorWrapper<IBindingSet>(
                left.iterator());

        // Buffer used to collect the solutions.
        final TestBuffer<IBindingSet> outputBuffer = new TestBuffer<IBindingSet>();
        
        // Compute the "required" solutions.
        JVMHashJoinUtility
                .hashJoin(leftItr, outputBuffer, joinVars, selectVars,
                        constraints, rightSolutions, optional, true/* leftIsPipeline */);

        if(optional) {
            
            // Output the optional solutions.
            JVMHashJoinUtility.outputOptionals(outputBuffer, rightSolutions);
            
        }

        // Verify the expected solutions.
        assertSameSolutionsAnyOrder(expected, outputBuffer.iterator());
        
    }

}
