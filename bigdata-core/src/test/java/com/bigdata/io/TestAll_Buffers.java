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
 * Created on Aug 26, 2009
 */

package com.bigdata.io;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Aggregates test suites in increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
       // test use of ByteBuffer as an input/output stream.
       TestByteBufferStreams.class,
       // test fixed-length record w/ absolute access.
       TestFixedByteArrayBuffer.class,
       // test extensible record w/ absolute and relative and stream-based
       // access.
       TestByteArrayBuffer.class,
       // test extensible record w/ DataOutput API.
       TestDataOutputBuffer.class,
       // test packed short support.
       TestShortPacker.class,
       // test packed long support.
       TestLongPacker.class
    } )
public class TestAll_Buffers {
}
