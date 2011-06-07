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
/*
 * Created on Aug 12, 2008
 */

package com.bigdata.rdf.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.ShortPacker;
import com.bigdata.io.compression.BOCU1Compressor;
import com.bigdata.io.compression.UnicodeHelper;
import com.bigdata.rdf.lexicon.ITermIndexCodes;

/**
 * Helper class provides efficient stand-off serialization of RDF {@link Value}
 * objects.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataValueSerializer<V extends Value> {

    /*
     * Serialization.
     */

    /**
     * Version zero(0) of the serialization format. This version could not write
     * out very large Unicode strings (64k character limit). Also, there were
     * are inefficiencies in {@link DataOutputBuffer} when writing UTF8 which
     * caused a performance hit. See {@link DataOutputBuffer#writeUTF(String)}.
     */
    private static final short VERSION0 = 0x0;

    /**
     * Version ONE(1) of the serialization format. This version uses BOCU-1 to
     * encode the data (BOCU-1 provides code point order preserving compressed
     * Unicode).
     */
    private static final short VERSION1 = 0x1;
    
    /**
     * The current serialization version.
     */
    private final static short currentVersion = VERSION1;
    
    /**
     * Error message indicates that the version code in the serialized
     * record did not correspond to a known serialization version for an RDF
     * value.
     */
    protected static final String ERR_VERSION = "Bad version";
    
    /**
     * Error message indicates that the term code in the serialized record
     * did not correspond to a known term code for an RDF value of the
     * appropriate type (e.g., a URI code where an Literal code was
     * expected). The codes are defined by {@link ITermIndexCodes}.
     */
    protected static final String ERR_CODE = "Bad term code";

    /**
     * Factory used to de-serialize {@link Value}s.
     */
    private final ValueFactory valueFactory;

    /**
     * Used to compress Unicode strings.
     */
    private final UnicodeHelper uc;
    
    /**
     * Create an instance that will materialize objects using the caller's
     * factory.
     * 
     * @param valueFactory
     *            The value factory.
     */
    public BigdataValueSerializer(final ValueFactory valueFactory) {

        if (valueFactory == null)
            throw new IllegalArgumentException();
        
        this.valueFactory = valueFactory;
       
        this.uc = new UnicodeHelper(new BOCU1Compressor());
        
    }

//    public byte getTermCode(final InputStream is) throws IOException {
//    	final short version = ShortPacker.unpackShort(is);
//    	switch(version){
//    	case VERSION0:
//		case VERSION1:
//			final int b = is.read();
//			if(b == -1)
//				throw new EOFException();
//			return (byte)(0xff & b);
//		default:
//			throw new AssertionError();
//		}
//    }
    
    /**
     * Return the term code as defined by {@link ITermIndexCodes} for this type
     * of term. This is used to places URIs, different types of literals, and
     * bnodes into disjoint parts of the key space for sort orders.
     * 
     * @see ITermIndexCodes
     */
    private byte getTermCode(final Value val) {
        
        if (val == null)
            throw new IllegalArgumentException();
        
        if (val instanceof URI) {
        
            return ITermIndexCodes.TERM_CODE_URI;
            
        } else if (val instanceof Literal) {
            
            final Literal lit = (Literal) val;
            
            if (lit.getLanguage() != null)
                return ITermIndexCodes.TERM_CODE_LCL;

            if (lit.getDatatype() != null)
                return ITermIndexCodes.TERM_CODE_DTL;

            return ITermIndexCodes.TERM_CODE_LIT;

        } else if (val instanceof BNode) {

            return ITermIndexCodes.TERM_CODE_BND;
            
        } else {
            
            throw new IllegalArgumentException("class="+val.getClass().getName());
            
        }

    }

    /**
     * Routine for efficient serialization of an RDF {@link Value}.
     * 
     * @return The byte[] containing the serialized data record.
     * 
     * @throws RuntimeException
     *             if there is a IO problem
     * 
     * @see {@link #deserialize(byte[])}
     */
    public byte[] serialize(final V val) {

        return serialize(val, new DataOutputBuffer(128), new ByteArrayBuffer(
                128));
        
    }

    /**
     * Variant which permits reuse of the same buffer. This has the advantage
     * that the buffer is reused on each invocation and swiftly grows to its
     * maximum extent.
     * 
     * @param val
     *            The value.
     * @param out
     *            The buffer - the caller is responsible for resetting the
     *            buffer before each invocation.
     * @param tmp
     *            A buffer used to compress the component Unicode strings. This
     *            will be reset as necessary by this method.
     * 
     * @return The byte[] containing the serialized data record. This array is
     *         newly allocated so that a series of invocations of this method
     *         return distinct byte[]s.
     */
    public byte[] serialize(final V val, final DataOutputBuffer out, final
            ByteArrayBuffer tmp) {
        
        try {

            final short version = currentVersion;

            ShortPacker.packShort(out, version);

            switch (version) {
            case VERSION0:
                serializeVersion0(val, version, out);
                break;
            case VERSION1:
                serializeVersion1(val, version, out, tmp);
                break;
            default:
                throw new UnsupportedOperationException(ERR_VERSION);
            }

            return out.toByteArray();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
                    
    }
    
    /**
     * Routine for efficient de-serialization of an RDF {@link Value}.
     * <p>
     * Note: This automatically uses the {@link BigdataValueFactoryImpl} to create
     * the {@link BigdataValue}s from the de-serialized state so the factory
     * reference is always set on the returned {@link BigdataValueImpl}.
     * 
     * @param b
     *            The byte[] containing the serialized data record.
     * 
     * @return The {@link BigdataValue}.
     * 
     * @throws RuntimeException
     *             if there is an IO problem.
     * 
     * @see {@link #serialize()}
     */
    public V deserialize(final byte[] b) {

        return deserialize(new DataInputBuffer(b), new StringBuilder(b.length));

    }

    /**
     * Routine for efficient de-serialization of a {@link BigdataValue}.
     * <p>
     * Note: This automatically uses the {@link BigdataValueFactoryImpl} to
     * create the {@link BigdataValue}s from the de-serialized state so the
     * factory reference is always set on the returned {@link BigdataValueImpl}.
     * 
     * @param b
     *            An input stream from which the serialized data may be read.
     * @param tmp
     *            A buffer used to decode the component Unicode strings. The
     *            length of the buffer will be reset as necessary by this
     *            method.
     * 
     * @return The {@link BigdataValue}.
     * 
     * @throws RuntimeException
     *             if there is an IO problem.
     * 
     * @see {@link #serialize()}
     */
    public V deserialize(final DataInputBuffer in, final StringBuilder tmp) {
        
        try {

            final short version = ShortPacker.unpackShort((DataInput)in);//in.unpackShort();

            switch (version) {
            case VERSION0:
                return deserializeVersion0(version, in);
            case VERSION1:
                return deserializeVersion1(version, in, tmp);
            default:
                throw new UnsupportedOperationException(ERR_VERSION + " : "
                        + version);
            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
         
    }

    /**
     * Implements the serialization of a Literal, URI, or BNode.
     * 
     * @param val
     *            The {@link Value}.
     * @param version
     *            The serialization version number (which has already been
     *            written on <i>out</i> by the caller).
     * @param out
     *            The data are written here.
     * 
     * @throws IOException
     */
    private void serializeVersion0(final V val, final short version,
            final DataOutput out) throws IOException {

        final byte termCode = getTermCode(val);

        /*
         * Note: VERSION0 writes the termCode immediately after the packed
         * version identifier. Other versions MAY do something else.
         * 
         * Note: This method requires the DataOutput interface for the
         * writeUTF() method. If we can get those _exact_ semantics elsewhere
         * (for backward compatibility they have to be exact) then we could
         * relax the API and pass in an OutputStream.
         */
        out.writeByte(termCode);

        switch(termCode) {
 
        case ITermIndexCodes.TERM_CODE_BND: {
            
            out.writeUTF(((BNode) val).getID());

            break;
        
        }

        case ITermIndexCodes.TERM_CODE_URI: {
            
            // Serialize as UTF.
            out.writeUTF(((URI)val).stringValue());

            break;
            
        }
        
        case ITermIndexCodes.TERM_CODE_LIT:
            
            out.writeUTF(((Literal)val).getLabel());
            
            break;
        
        case ITermIndexCodes.TERM_CODE_LCL:

            /*
             * Note: This field is ASCII [A-Za-z0-9] and "-". However, this
             * method writes using UTF-8 so it will generate one byte per
             * character and it is probably more work to write the data
             * directly as ASCII bytes.
             */
            
            out.writeUTF(((Literal)val).getLanguage());
            
            out.writeUTF(((Literal)val).getLabel());
            
            break;
        
        case ITermIndexCodes.TERM_CODE_DTL:
            
            out.writeUTF(((Literal)val).getDatatype().stringValue());

            out.writeUTF(((Literal)val).getLabel());

            break;

        default:
        
            throw new IOException(ERR_CODE + " : " + termCode);
        
        }

    }
    
    /**
     * Implements the de-serialization of a Literal, URI, or BNode.
     * <p>
     * Note: This automatically uses the {@link BigdataValueFactoryImpl} to create
     * the {@link BigdataValue}s from the de-serialized state so the factory
     * reference is always set on the returned {@link BigdataValueImpl}.
     * 
     * @param version
     *            The serialization version number (which has already been read
     *            by the caller).
     * @param in
     *            The data are read from here.
     * 
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private V deserializeVersion0(final short version, final DataInput in)
            throws IOException {
    
        /*
         * Note: The term code immediately follows the packed version
         * code for VERSION0 - this is not necessarily true for other
         * serialization versions.
         */

        final byte termCode = in.readByte();

        switch(termCode) {
        
        case ITermIndexCodes.TERM_CODE_BND: {

            return (V) valueFactory.createBNode(in.readUTF());

        }

        case ITermIndexCodes.TERM_CODE_URI: {

            return (V) valueFactory.createURI(in.readUTF());

        }

        case ITermIndexCodes.TERM_CODE_LIT: {

            final String label = in.readUTF();

            return (V)valueFactory.createLiteral(label);

        }
        
        case ITermIndexCodes.TERM_CODE_LCL: {

            final String language = in.readUTF();
            final String label = in.readUTF();

            return (V)valueFactory.createLiteral(label, language);
        }
        
        case ITermIndexCodes.TERM_CODE_DTL: {

            final String datatype = in.readUTF();

            final String label = in.readUTF();

            return (V) valueFactory.createLiteral(label, valueFactory
                    .createURI(datatype));
            
        }

        default:

            throw new IOException(ERR_CODE + " : " + termCode);

        }
        
    }

    /**
     * Implements the serialization of a Literal, URI, or BNode.
     * 
     * @param val
     *            The {@link Value}.
     * @param version
     *            The serialization version number (which has already been
     *            written on <i>out</i> by the caller).
     * @param out
     *            The data are written here.
     * @param tmp
     *            A buffer used to compress the component Unicode strings.
     * 
     * @throws IOException
     */
    private void serializeVersion1(final V val, final short version,
            final OutputStream out, final ByteArrayBuffer tmp)
            throws IOException {

        final byte termCode = getTermCode(val);

        /*
         * Note: VERSION1 writes the termCode immediately after the packed
         * version identifier. Other versions MAY do something else.
         */
        out.write/*Byte*/(termCode);

        switch(termCode) {
 
        case ITermIndexCodes.TERM_CODE_BND:
            
            uc.encode(((BNode) val).getID(), out, tmp);
            
            break;

        case ITermIndexCodes.TERM_CODE_URI:
            
            uc.encode(((URI)val).stringValue(), out, tmp);
            
            break;
        
        case ITermIndexCodes.TERM_CODE_LIT:

            uc.encode(((Literal)val).getLabel(), out, tmp);
            
            break;
        
        case ITermIndexCodes.TERM_CODE_LCL:

            /*
             * Note: This field is ASCII [A-Za-z0-9] and "-". However, this
             * method writes using UTF-8 so it will generate one byte per
             * character and it is probably more work to write the data
             * directly as ASCII bytes.
             */
            
            uc.encode(((Literal) val).getLanguage(), out, tmp);

            uc.encode(((Literal) val).getLabel(), out, tmp);

            break;
        
        case ITermIndexCodes.TERM_CODE_DTL:

            uc.encode(((Literal) val).getDatatype().stringValue(), out, tmp);

            uc.encode(((Literal) val).getLabel(), out, tmp);

            break;

        default:
        
            throw new IOException(ERR_CODE + " : " + termCode);
        
        }

    }

    /**
     * Implements the de-serialization of a Literal, URI, or BNode.
     * <p>
     * Note: This automatically uses the {@link BigdataValueFactoryImpl} to
     * create the {@link BigdataValue}s from the de-serialized state so the
     * factory reference is always set on the returned {@link BigdataValueImpl}.
     * 
     * @param version
     *            The serialization version number (which has already been read
     *            by the caller).
     * @param in
     *            The data are read from here.
     * @param tmp
     *            Buffer used to extract bytes to be decompressed.
     * @param sb
     *            Buffer used to decompress bytes.
     * 
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private V deserializeVersion1(final short version,
            final DataInputBuffer in, final StringBuilder tmp)
            throws IOException {

        /*
         * Note: The term code immediately follows the packed version code for
         * VERSION0 - this is not necessarily true for other serialization
         * versions.
         */

        final byte termCode = in.readByte();

        switch (termCode) {

        case ITermIndexCodes.TERM_CODE_BND:
            return (V) valueFactory.createBNode(uc.decode1(in, tmp));

        case ITermIndexCodes.TERM_CODE_URI:
            return (V) valueFactory.createURI(uc.decode1(in, tmp));

        case ITermIndexCodes.TERM_CODE_LIT:
            return (V) valueFactory.createLiteral(uc.decode1(in, tmp));

        case ITermIndexCodes.TERM_CODE_LCL: {

            final String language = uc.decode1(in, tmp);

            final String label = uc.decode1(in, tmp);

            return (V) valueFactory.createLiteral(label, language);
        }

        case ITermIndexCodes.TERM_CODE_DTL: {

            final String datatype = uc.decode1(in, tmp);

            final String label = uc.decode1(in, tmp);

            return (V) valueFactory.createLiteral(label, valueFactory
                    .createURI(datatype));

        }

        default:

            throw new IOException(ERR_CODE + " : " + termCode);

        }

    }

}
