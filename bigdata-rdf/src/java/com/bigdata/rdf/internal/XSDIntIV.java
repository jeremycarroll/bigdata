/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:int</code>. */
public class XSDIntIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, Integer> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final int value;

    public XSDIntIV(final int value) {
        
        super(DTE.XSDInt);
        
        this.value = value;
        
    }

    final public Integer getInlineValue() {
        
        return value;
        
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        final V v = (V) f.createLiteral(value);
        v.setIV(this);
        return v;
    }

    @Override
    final public long longValue() {
        return (long) value;
    }

    @Override
    public boolean booleanValue() {
        return value == 0 ? false : true;
    }

    @Override
    public byte byteValue() {
        return (byte) value;
    }

    @Override
    public double doubleValue() {
        return (double) value;
    }

    @Override
    public float floatValue() {
        return (float) value;
    }

    @Override
    public int intValue() {
        return value;
    }

    @Override
    public short shortValue() {
        return (short) value;
    }
    
    @Override
    public String stringValue() {
        return Integer.toString(value);
    }

    @Override
    public BigDecimal decimalValue() {
        return BigDecimal.valueOf(value);
    }

    @Override
    public BigInteger integerValue() {
        return BigInteger.valueOf(value);
    }

    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof XSDIntIV<?>) {
            return this.value == ((XSDIntIV<?>) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the int value.
     * 
     * @see Integer#hashCode()
     */
    public int hashCode() {
        return value;
    }

    public int byteLength() {
        return 1 + Bytes.SIZEOF_INT;
    }

    @Override
    protected int _compareTo(IV o) {
         
        final int value2 = ((XSDIntIV) o).value;
        
        return value == value2 ? 0 : value < value2 ? -1 : 1;
        
    }
    
}