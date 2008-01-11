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
/*
 * Created on May 21, 2007
 */
package com.bigdata.rdf.store;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.IndexProcedure;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;

/**
 * Unisolated write operation makes consistent assertions on the
 * <em>ids</em> index based on the data developed by the {@link AddTerms}
 * operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AddIds extends IndexProcedure {

    /**
     * 
     */
    private static final long serialVersionUID = -5480378815444534653L;

    /**
     * De-serialization constructor.
     */
    public AddIds() {
        
    }
    
    public AddIds(int n, int offset, byte[][] keys, byte[][] vals) {

        super(n, offset, keys, vals);
        
        assert vals != null;
        
    }
    
    /**
     * Conditionally inserts each key-value pair into the index. The keys
     * are the term identifiers. The values are the terms as serialized by
     * {@link _Value#serialize()}. Since a conditional insert is used, the
     * operation does not cause terms that are already known to the ids
     * index to be re-inserted, thereby reducing writes of dirty index
     * nodes.
     * 
     * @param ndx
     *            The index.
     * 
     * @return <code>null</code>.
     */
    public Object apply(IIndex ndx) {
        
        final int n = getKeyCount();
        
        for(int i=0; i<n; i++) {
    
            final byte[] key = getKey(i);
            
            final byte[] val;

            /*
             * Note: Validation SHOULD be disabled except for testing.
             * 
             * FIXME turn off validation for release or performance testing.
             */
            final boolean validate = false;
            
            if (validate) {

                /*
                 * When the term identifier is found in the reverse mapping
                 * this code path validates that the serialized term is the
                 * same.
                 */
                byte[] oldval = (byte[]) ndx.lookup(key);
                
                val = getValue(i);
                
                if( oldval == null ) {
                    
                    if (ndx.insert(key, val) != null) {

                        throw new AssertionError();

                    }
                    
                } else {

                    /*
                     * Note: This would fail if the serialization of the
                     * term was changed. In order to validate when different
                     * serialization formats might be in use you have to
                     * actually deserialize the terms. However, I have the
                     * validation logic here just as a santity check while
                     * getting the basic system running - it is not meant to
                     * be deployed.
                     */

                    if (! BytesUtil.bytesEqual(val, oldval)) {

                        throw new RuntimeException(
                                "Consistency problem: id="
                                        + KeyBuilder.decodeLong(key, 0));
                        
                    }
                    
                }
                
            } else {
                
                /*
                 * This code path does not validate that the term identifier
                 * is mapped to the same term. This is the code path that
                 * you SHOULD use.
                 */

                if (!ndx.contains(key)) {

                    val = getValue(i);
                    
                    if (ndx.insert(key, val) != null) {

                        throw new AssertionError();

                    }

                }

            }
            
        }
        
        return null;
        
    }

}
