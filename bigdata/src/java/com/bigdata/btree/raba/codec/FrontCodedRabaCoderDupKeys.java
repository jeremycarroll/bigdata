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
package com.bigdata.btree.raba.codec;

/**
 * Variant of the {@link FrontCodedRabaCoder} that supports duplicate keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class FrontCodedRabaCoderDupKeys extends FrontCodedRabaCoder {

    /**
     * A default instance. 
     */
    public static final transient FrontCodedRabaCoderDupKeys INSTANCE = new FrontCodedRabaCoderDupKeys();

    protected transient static final int DEFAULT_RATIO = 8;

    public FrontCodedRabaCoderDupKeys(final int ratio) {

        super(ratio);
        
    }

    public FrontCodedRabaCoderDupKeys() {
        
        super(DEFAULT_RATIO);
        
    }

    @Override
    public boolean isDuplicateKeys() {

        return true;
        
    }

}
