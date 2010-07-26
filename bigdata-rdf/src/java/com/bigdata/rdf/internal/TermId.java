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

import java.util.UUID;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Implementation for any kind of RDF Value when the values is not being
 * inlined. Instances of this class can represent URIs, Blank Nodes (if they are
 * not being inlined), Literals (including datatype literals if they are not
 * being inlined) or SIDs (statement identifiers).
 */
public class TermId<V extends BigdataValue/* URI,BNode,Literal,SID */>
        extends AbstractIV<V, Void> {

    /**
     * 
     */
    private static final long serialVersionUID = 4309045651680610931L;
    
    /**
     * Value used for a "NULL" term identifier.
     */
    public static final transient long NULL = 0L;

    /** The term identifier. */
    private final long termId;

//    /** The datatype term identifier. */
//    private final long dataTypeId;

    /**
     * Constructor for a term identifier when you are decoding and already have
     * the flags.
     * 
     * @param flags
     *            The flags
     * @param termId
     */
    public TermId(final byte flags, final long termId) {

        super(flags);

        this.termId = termId;
        
//        this.dataTypeId = 0L;
        
    }

    /**
     * Constructor for a term identifier.
     * @param vte
     * @param termId
     */
    public TermId(final VTE vte, final long termId) {

        /*
         * Note: XSDBoolean happens to be assigned the code value of 0, which is
         * the value we we want when the data type enumeration will be ignored.
         */
        super(vte, false/* inline */, false/* extension */, DTE.XSDBoolean);

        this.termId = termId;
        
//        this.dataTypeId = 0L;
        
    }
    
//    /**
//     * Constructor for a term identifier for a datatype literal. Do NOT use this
//     * constructor when the lexicon is configured such that the datatype literal
//     * should be inlined.
//     * 
//     * @param vte
//     * @param dte
//     * @param termId
//     * @param dataTypeId
//     */
//    public TermId(final VTE vte, final DTE dte, final long termId,
//            final long dataTypeId) {
//
//        super(vte, false/* inline */, true/* extension */, dte);
//
//        if (dataTypeId == IRawTripleStore.NULL)
//            throw new IllegalArgumentException();
//        
//        this.termId = termId;
//        
//        this.dataTypeId = dataTypeId;
//
//    }

    /**
     * Human readable representation includes the term identifier, whether
     * this is a URI, Literal, Blank node, or Statement identifier and the
     * datatype URI if one is assigned. This representation is based solely
     * on the flag bits and the term identifier.
     */
    public String toString() {

        return "TermId(" + termId + 
                String.valueOf(getVTE().getCharCode()) + ")";
//                + (datatype == null ? "" : datatype);

    }

    /**
     * {@inheritDoc}
     * <p>
     * {@link TermId}s are never inline so this method always throws an
     * exception.
     * 
     * @throws UnsupportedOperationException
     *             always since {@link TermId}s are never inline.
     */
    final public V asValue(final BigdataValueFactory f, 
            final ILexiconConfiguration config)
            throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    final public Void getInlineValue() {
        throw new UnsupportedOperationException();
    }

    final public long getTermId() {
        return termId;
    }
    
//    /**
//     * Return the term identifier for the datatype associated with the term.
//     */
//    final public long getDataTypeID() {
//        return dataTypeId;
//    }

    /**
     * Always returns <code>false</code> since the RDF value is not inline.
     */
    @Override
    final public boolean isInline() {
        return false;
    }

    /**
     * Always returns <code>true</code> since this is a term identifier.
     */
    @Override
    final public boolean isTermId() {
        return true;
    }

    /**
     * Note: only the termId matters for equality (unless we also have a
     * transient reference to the value factory which stands in as a proxy for
     * the KB instance).
     */
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof TermId<?>) {
            return termId == ((TermId<?>) o).termId;
        }
        return false;
    }

    /**
     * Return the hash code of the long term identifier.
     * 
     * @see Long#hashCode()
     */
    public int hashCode() {
        return (int) (termId ^ (termId >>> 32));
    }
    
    /**
     * {@inheritDoc}
     * 
     * FIXME We are going to need another class with similar semantics if we
     * also allow raw tids (the historical way of coding a term identifier as
     * just 8 bytes). I suggest RawTermId for the old way and TermId for the new
     * way.
     */
    public int byteLength() {

        return 1 + Bytes.SIZEOF_LONG;
        
    }

    @Override
    protected int _compareTo(IV o) {
        
        final long termId2 = ((TermId<?>) o).termId;
        
        return termId < termId2 ? -1 : termId > termId2 ? 1 : 0; 
        
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public IKeyBuilder encode(final IKeyBuilder keyBuilder) {

        // First emit the flags byte.
        keyBuilder.append(flags());

        keyBuilder.append(getTermId());
        
        return keyBuilder;
        
    }
    

}
