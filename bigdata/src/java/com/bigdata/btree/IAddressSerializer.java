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
 * Created on Dec 26, 2006
 */

package com.bigdata.btree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Interface for (de-)serialization of addresses of child nodes and leaves as
 * recorded on a node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see NodeSerializer
 */
public interface IAddressSerializer {

    /**
     * The maximum size of a sequence of serialized addresses in bytes. This is
     * used to compute the maximum required size of a buffer to (de-)serialize a
     * node.
     * 
     * @param n
     *            The #of addresses in the sequence.
     */
    public int getSize(int n);

    /**
     * De-serialize the child addresses for a node.
     * 
     * @param is
     *            The input stream.
     * @param childAddr
     *            The array into which the addresses must be written.
     * @param nchildren
     *            The #of valid values in the array. The values in indices
     *            [0:n-1] are defined and must be read from the buffer and
     *            written on the array.
     */
    public void getChildAddresses(DataInput is, long[] childAddr,
            int nchildren) throws IOException;

    /**
     * Serialize the child addresses for a node.
     * 
     * @param os
     *            The output stream.
     * @param childAddr
     *            The array of child addresses to be written.
     * @param nchildren
     *            The #of valid values in the array. The values in indices
     *            [0:n-1] are defined and must be written.
     */
    public void putChildAddresses(DataOutputBuffer os, long[] childAddr,
            int nchildren) throws IOException;

}
