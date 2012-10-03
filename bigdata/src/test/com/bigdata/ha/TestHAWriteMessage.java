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
package com.bigdata.ha;

import java.io.IOException;

import junit.framework.TestCase;

import com.bigdata.btree.BytesUtil;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.journal.ha.HAWriteMessage;

public class TestHAWriteMessage extends TestCase {

	/**
	 * Simple test to verify HAWriteMessage serialization
	 */
    public void testSerialization() throws IOException, ClassNotFoundException {

        final HAWriteMessage msg1 = new HAWriteMessage(
                12L,// commitCounter
                13L,// commitTime
                14L,// sequence
                15,// size
                16,// checksum
                StoreTypeEnum.RW,//
                17L,// quorumToken
                18L,// fileExtent
                19L // firstOffset
                );

        final byte[] ser1 = serialized(msg1);

        final HAWriteMessage msg2 = (HAWriteMessage) SerializerUtil
                .deserialize(ser1);

        assertTrue(msg1.equals(msg2));

        // now confirm serialized byte equivalence in case we just messed up
        // equals
        final byte[] ser2 = serialized(msg2);

        assertTrue(BytesUtil.bytesEqual(ser1, ser2));

//        System.err.println("msg1: " + msg1);
//        System.err.println("msg2: " + msg2);

    }
	
	/**
	 * Utility to return byte[] serialization of the HAWriteMessage
	 */
    private byte[] serialized(final HAWriteMessage msg) {

	    return SerializerUtil.serialize(msg);
	}

}
