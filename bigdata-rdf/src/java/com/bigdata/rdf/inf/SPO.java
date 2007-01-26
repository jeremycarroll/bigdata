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
package com.bigdata.rdf.inf;

import com.bigdata.rdf.KeyOrder;
import com.bigdata.rdf.RdfKeyBuilder;

/**
 * Represents a triple.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPO {
    
    /**
     * @see RdfKeyBuilder#CODE_STMT
     * @see RdfKeyBuilder#CODE_PRED
     * @see RdfKeyBuilder#CODE_RULE
     */
    public final byte code;
    public final long s;
    public final long p;
    public final long o;
    
    /**
     * Construct a triple from term identifiers.
     * 
     * @param s
     * @param p
     * @param o
     */
    public SPO(long s, long p, long o) {
        this.code = RdfKeyBuilder.CODE_STMT;
        this.s = s;
        this.p = p;
        this.o = o;
    }
    
    /**
     * Construct a triple from the sort key.
     * 
     * @param keyOrder
     *            Indicates the permutation of the subject, predicate and object
     *            used by the key.
     * 
     * @param keyBuilder
     *            Used to decode the key.
     * 
     * @param key
     *            The key.
     * 
     * @see RdfKeyBuilder#key2Statement(byte[], long[])
     */
    public SPO(KeyOrder keyOrder, RdfKeyBuilder keyBuilder, byte[] key) {
        
        long[] ids = new long[3];
        
        code = keyBuilder.key2Statement(key, ids); 
        
        switch (keyOrder) {

        case SPO:
            s = ids[0];
            p = ids[1];
            o = ids[2];

            break;
        case POS:
            p = ids[0];
            o = ids[1];
            s = ids[2];

            break;
        case OSP:
            o = ids[0];
            s = ids[1];
            p = ids[2];

            break;

        default:

            throw new UnsupportedOperationException();

        }

    }

}
