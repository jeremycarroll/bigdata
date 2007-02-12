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
 * Created on Feb 12, 2007
 */

package com.bigdata.objndx;

/**
 * Data for a batch insert operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BatchInsert implements IBatchOp {

    /**
     * The #of tuples to be processed.
     */
    public final int ntuples;
    
    /**
     * The keys for each tuple.
     */
    public final byte[][] keys;
    
    /**
     * The value corresponding to each key.
     */
    public final Object[] values;
    
    /**
     * The index of the tuple that is currently being processed.
     */
    public int tupleIndex = 0;
    
    /**
     * 
     * @param ntuples
     *            The #of tuples that are being inserted(in).
     * @param keys
     *            The array of searck keys (one key per tuple) (in).
     * @param values
     *            Values (one element per key) (in/out). Null elements are
     *            allowed on input and will cause a null value to be associated
     *            with the key. On output, each element is either null (if there
     *            was no entry for that key) or the old value stored under that
     *            key (which may be null).
     */
    public BatchInsert(int ntuples, byte[][] keys, Object[] values) {

        if (ntuples <= 0)
            throw new IllegalArgumentException(Errors.ERR_NTUPLES_NON_POSITIVE);
            
        if (keys == null)
            throw new IllegalArgumentException(Errors.ERR_KEYS_NULL);

        if( keys.length < ntuples )
            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_KEYS);

        if (values == null)
            throw new IllegalArgumentException(Errors.ERR_VALS_NULL);

        if( values.length < ntuples )
            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_VALS);

        this.ntuples = ntuples;
        this.keys = keys;
        this.values = values;
        
    }
    
}
