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
/*
 * Created on June 3rd, 2011
 */
package com.bigdata.rdf.internal;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * A fully inlined representation of a URI based on a <code>byte</code> code.
 * The flags byte looks like: <code>VTE=URI, inline=true, extension=false,
 * DTE=XSDByte</code>. It is followed by an <code>unsigned byte</code> value
 * which is the index of the URI in the {@link Vocabulary} class for the triple
 * store.
 * 
 * @author thompsonbry
 */
public class URIByteIV<V extends BigdataURI> extends AbstractInlineIV<V, Byte> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	final private byte value;

	byte byteValue() {
		
		return value;
		
	}
	
	public URIByteIV(final byte value) {

		super(VTE.URI, DTE.XSDByte);

		this.value = value;

	}

	@SuppressWarnings("unchecked")
	@Override
	protected int _compareTo(final IV o) {

        final byte v = KeyBuilder.decodeByte(value);

        final byte v2 = KeyBuilder
                .decodeByte(((URIByteIV<BigdataURI>) o).value);

        return v == v2 ? 0 : v < v2 ? -1 : 1;

    }

	@Override
	public boolean equals(final Object o) {
		if (this == o)
			return true;
		if (o instanceof URIByteIV<?>) {
			return this.value == ((URIByteIV<?>) o).value;
		}
        return false;
    }

    /**
     * Return the hash code of the byte value.
     */
	final public int hashCode() {
		
		return (int) value;
		
	}

	public V asValue(final LexiconRelation lex)
			throws UnsupportedOperationException {

    	V v = getValueCache();
		
    	if (v == null) {
    		
			final BigdataValueFactory f = lex.getValueFactory();
			
			v = (V) lex.getContainer().getVocabulary().asValue(this);
			
			v.setIV(this);
			
			setValue(v);
			
		}

    	return v;
		
	}

	final public int byteLength() {

		return 2 /* flags(1) + byte(2) */;
		
	}

	final public Byte getInlineValue() {
	
		return value;
		
	}

	public String stringValue() {
		
		return "Vocab(" + Byte.toString(value) + ")";
		
	}

}
