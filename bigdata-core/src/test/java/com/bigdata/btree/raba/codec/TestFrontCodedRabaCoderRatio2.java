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
 * Created on Aug 6, 2009
 */

package com.bigdata.btree.raba.codec;

import org.junit.Before;


/**
 * Test suite for the {@link FrontCodedRabaCoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFrontCodedRabaCoderRatio2 extends AbstractFrontCodedRabaCoderTestCase {

    /**
     * 
     */
    public TestFrontCodedRabaCoderRatio2() {
    }

    @Before
    public void setUp() throws Exception {
        
        rabaCoder = new FrontCodedRabaCoder(2/* ratio */);
        
    }

}
