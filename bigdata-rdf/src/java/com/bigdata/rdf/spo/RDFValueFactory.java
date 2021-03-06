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
package com.bigdata.rdf.spo;

/**
 * Factory for the single element <code>byte[]</code> used for the value of an
 * RDF Statement in one of the statement indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class RDFValueFactory {

    private final static byte[][] table = createStaticByteArrayTable();

    private static byte[][] createStaticByteArrayTable() {
        final byte[][] table = new byte[256][];

        for (int i = 0; i < 256; i++) {

            table[i] = new byte[] { (byte) i };

        }

        return table;

    }

    /**
     * Return the B+Tree value for an RDF Statement given its byte value.
     * 
     * @param i
     *            The byte value of the Statement.
     *            
     * @return A byte[] whose sole element is that byte value.
     */
    static public byte[] getValue(final byte i) {

        return table[i];

    }
    
}
