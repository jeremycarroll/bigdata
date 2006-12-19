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
 * Created on Dec 19, 2006
 */

package com.bigdata.objndx;

import java.nio.ByteBuffer;

import com.bigdata.journal.IRawStore;
import com.bigdata.journal.ISlotAllocation;

/**
 * Transitional interface bridging {@link IRawStore} based on fixed size slot
 * allocations to a new interface drops the concept of {@link ISlotAllocation}
 * and encodes the #of bytes (vs #of slots) directly into the long {@link Addr}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IRawStore
 */
public interface IRawStore2 {

    /**
     * Reads data, assembling the result in a buffer. This method is not
     * isolated.
     * 
     * @param off
     *            The offset from which the data will be read.
     * @param len
     *            The #of bytes to be read.
     * @param dst
     *            The destination buffer (optional). When specified, the data
     *            will be appended starting at the current position. If there is
     *            not enough room in the buffer then a new buffer will be
     *            allocated and used for the read operation. In either case, the
     *            position will be advanced as a side effect and the limit will
     *            equal the final position.
     * 
     * @return The data read. A new buffer will be allocated if <i>dst</i> is
     *         <code>null</code> -or- if the data will not fit in the provided
     *         buffer.
     */
    public ByteBuffer read(long addr, ByteBuffer dst);

    public void delete(long addr);
    
    /**
     * Writes the data, returns the {@link Addr} as a long integer.
     * 
     * @param data
     *            The data.
     *            
     * @return The address that can be used to recover the data.
     */
    public long write(ByteBuffer data);
    
}
