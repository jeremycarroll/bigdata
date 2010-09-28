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
package com.bigdata.btree.raba.codec;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Aggregates test suites into increasing dependency order.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
       // no data (discards any values).
       TestEmptyRabaCoder.class,

       // no compression.
       TestSimpleRabaCoder.class,

       // front-compression for ordered unsigned byte[]s.
       TestFrontCodedRabaCoderRatio2.class,
       TestFrontCodedRabaCoderRatio8.class,
       TestFrontCodedRabaCoderRatio32.class,

       // canonical huffman coding.
       TestCanonicalHuffmanRabaCoder.class,

       /*
        * Tests of conditional raba coders (one coder is used when there are LT
        * N entries, otherwise the other coder is used).
        */
       TestConditionalRabaCoder_keys_simple_frontCoded.class,
       TestConditionalRabaCoder_values_simple_canonical.class
       } )
public class TestAll {

    /**
     * 
     */
    public TestAll() {
    }
}
