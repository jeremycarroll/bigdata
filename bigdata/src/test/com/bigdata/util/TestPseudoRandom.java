/*

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

package com.bigdata.util;

import junit.framework.TestCase;

public class TestPseudoRandom extends TestCase {
	
	public void testSimpleGen() {
		testRange(7);
		testRange(123);
		testRange(25764);
		testRange(58000);
	}
	
	void testRange(final int range) {
		byte[] tst = new byte[range];
		
		PseudoRandom psr = new PseudoRandom(range);
		
		for (int i = 0; i < range; i++) {
			// we want to test 0 - (range-1)
			final int nxt = psr.next()-1;
			assertTrue(nxt <= range);
			assertTrue(tst[nxt] == 0);
			
			tst[nxt] = 1;
		}
		
	}

}
