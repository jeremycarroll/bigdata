/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
package com.bigdata.rdf.internal.impl.bnode;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.math.BigInteger;

import org.openrdf.model.BNode;
import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.LongPacker;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.AbstractInlineIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.spo.SPOKeyOrder;

/**
 * Internal value representing an inline statement identifier. Uses the
 * {@link ISPO} supplied in the ctor as the inline value. The
 * {@link #asValue(BigdataValueFactory, ILexiconConfiguration)} method returns a
 * {@link BigdataBNode} that is used to represent the sid in serialization
 * formats (such as the custom RDF/XML extension for sids). The bnode is
 * guaranteed to always have the same bnode id for a given inlined SPO. This is
 * accomplished using the byte[] key encoding for the spo along with the
 * BigInteger class.
 * <p>
 * This internal value has a {@link VTE} of {@link VTE#STATEMENT}. It is encoded
 * into the statement indices by directly encoding the spo using
 * {@link SPOKeyOrder#encodeKey(IKeyBuilder, ISPO)} via the
 * {@link SPOKeyOrder#SPO} key order. Thus when decoded from the statement
 * indices, the spo associated with this sid is materialized directly from the
 * sid itself. See {@link IVUtility#decode(byte[])}. The spo decoded from the
 * sid IV will be marked as explicit (only explicit statements have sids) and
 * this SidIV will be attached to it. This completely eliminates the need for a
 * reverse index from sid->spo, as the spo is encoded inline into the SidIV
 * itself.  This replaces the TermId model for representing sids.
 * <p>
 * {@inheritDoc}
 */
public class SidIV<V extends BigdataBNode> extends AbstractInlineIV<V, ISPO>
        implements Serializable, BNode {

    /**
	 * 
	 */
	private static final long serialVersionUID = 685148537376856907L;
	
//	private static final transient Logger log = Logger.getLogger(SidIV.class);

	/**
	 * The inline spo.
	 */
	private final ISPO spo;
	
	/**
	 * The cached byte[] key for the encoding of this IV.
	 */
	private transient byte[] key;
	
	/**
	 * The cached materialized BigdataValue for this sid.
	 */
	private transient V bnode;

    public IV<V, ISPO> clone(final boolean clearCache) {

        final SidIV<V> tmp = new SidIV<V>(spo);

        // Propagate the cached byte[] key.
        tmp.key = key;
        
        // Propagate the cached BigdataValue.
        tmp.bnode = bnode;
        
        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }

    /**
	 * Ctor with internal value spo specified.
	 */
	public SidIV(final ISPO spo) {

        /*
         * Note: XSDBoolean happens to be assigned the code value of 0, which is
         * the value we we want when the data type enumeration will be ignored.
         */
        super(VTE.STATEMENT, DTE.XSDBoolean);
        
        this.spo = spo;
        
    }

	/**
	 * Returns the inline spo.
	 */
	public ISPO getInlineValue() throws UnsupportedOperationException {
		return spo;
	}

	/**
	 * Returns the bnode representation of this IV, useful for serialization
	 * formats such as RDF/XML.  See {@link #bnodeId()}.
	 */
    public V asValue(final LexiconRelation lex) {
    	if (bnode == null) {
	        bnode = (V) lex.getValueFactory().createBNode(getID());
	        bnode.setIV(this);
	        bnode.setStatementIdentifier(true);
    	}
        return bnode;
    }

    /**
     * Return the byte length for the byte[] encoded representation of this
     * internal value.  Depends on the byte length of the encoded inline spo.
     */
	public int byteLength() {
		return 1 + key().length;
	}

	public String toString() {
		return "Sid("+toString(spo)+")";
	}
	
	/**
	 * Pretty print the inline spo.  Calling SPO.toString() results in an
	 * infinite loop.
	 */
	private static String toString(final ISPO spo) {
        return (SPO.toString(spo.s()) + ":" + 
        		SPO.toString(spo.p()) + ":" + 
        		SPO.toString(spo.o()));
	}

	public int hashCode() {
		return spo.hashCode();
	}

    /**
     * Implements {@link BNode#getID()}.
     * <p>
     * This implementation uses the {@link BigInteger} class to create a unique
     * blank node ID based on the <code>unsigned byte[]</code> key of the inline
     * {@link SPO}.
     */
	@Override
	public String getID() {
//		// just use the hash code.  can result in collisions
//		return String.valueOf(hashCode());
		
		// create a big integer using the spo key.  should result in unique ids
		final byte[] key = key();
		final int signum = key.length > 0 ? 1 : 0;
		final BigInteger bi = new BigInteger(signum, key);
		return bi.toString();
	}

	public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o instanceof SidIV) {
    		final ISPO spo2 = ((SidIV) o).spo;
            return spo.equals(spo2);
        }
        return false;
	}

	public int _compareTo(IV o) {

	    /*
	     * Note: This works, but it might be more expensive.
	     */
//	    return UnsignedByteArrayComparator.INSTANCE.compare(key(), ((SidIV)o).key());

        /*
         * This should work as soon as we fix the other IV Comparable
         * implementations.
         */
        final ISPO spo2 = ((SidIV) o).spo;
        
        return SPOComparator.INSTANCE.compare(spo, spo2);
        
	}
	
    /**
     * Encode this internal value into the supplied key builder.  Emits the
     * flags, following by the encoded byte[] representing the spo, in SPO
     * key order.
     * <p>
     * {@inheritDoc}
     */
	@Override
    public IKeyBuilder encode(final IKeyBuilder keyBuilder) {

        // First emit the flags byte.
        keyBuilder.appendSigned(flags());
		
		// Then append the SPO's key in SPOKeyOrder.SPO
        keyBuilder.append(key());
        
        return keyBuilder;
            
    }
    
    private byte[] key() {
    	if (key == null) {
            /*
    		 * Build the SPO's key in SPOKeyOrder.SPO.
    		 */
    		key = SPOKeyOrder.SPO.encodeKey(new KeyBuilder(), spo);
    	}
    	return key;
    }

    /**
     * Object provides serialization for {@link SidIV} via the write-replace
     * and read-replace pattern.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static class SidIVState implements Externalizable {

//        private byte flags;
        private byte[] key;
        
        /**
         * De-serialization constructor.
         */
        public SidIVState() {
            
        }
        
        private SidIVState(final SidIV iv) {
//            this.flags = flags;
            this.key = iv.key();
        }
        
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
//            flags = in.readByte();
            final int nbytes = LongPacker.unpackInt(in);
            key = new byte[nbytes];
            in.readFully(key);
        }

        public void writeExternal(ObjectOutput out) throws IOException {
//            out.writeByte(flags);
            LongPacker.packLong(out, key.length);
            out.write(key);
        }
        
        private Object readResolve() throws ObjectStreamException {

            final ISPO spo = SPOKeyOrder.SPO.decodeKey(key);
            
            // SIDs are always explicit statements.
            spo.setStatementType(StatementEnum.Explicit);
            
            return new SidIV(spo);
            
        }

    }
    
    private Object writeReplace() throws ObjectStreamException {
        
        return new SidIVState(this);
        
    }
    
    /**
     * Implements {@link Value#stringValue()}.
     */
    @Override
    public String stringValue() {
        
        return getID();

    }

    /**
     * Does not need materialization to answer BNode interface methods.
     */
	@Override
	public boolean needsMaterialization() {
		
		return false;
		
	}


}