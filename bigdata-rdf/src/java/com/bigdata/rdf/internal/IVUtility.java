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
 * Created on May 3, 2010
 */

package com.bigdata.rdf.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.NullOutputStream;
import com.bigdata.io.compression.BOCU1Compressor;
import com.bigdata.io.compression.UnicodeHelper;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.lexicon.ITermIndexCodes;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOKeyOrder;

/**
 * Helper class for {@link IV}s.
 */
public class IVUtility {

	private static final transient Logger log = Logger.getLogger(IVUtility.class);
	
	/**
	 * Helper instance for compression/decompression of Unicode string data.
	 */
	static UnicodeHelper un = new UnicodeHelper(new BOCU1Compressor());

    /**
     * Return the byte length of the compressed representation of a unicode
     * string.
     * <p>
     * This does all the work to compress something and then returns you the
     * length. Caller's need to be smart about how and when they find the byte
     * length of an IV representing some inlined Unicode data since we basically
     * have to serialize the String to find the length.
     * 
     * @param s
     *            The string.
     * 
     * @return Its compressed byte length.
     */
    static int byteLengthUnicode(final String s) {
        try {
            return un.encode(s, new NullOutputStream(), new ByteArrayBuffer(s
                    .length()));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
	
    public static boolean equals(IV iv1, IV iv2) {
        
        // same IV or both null
        if (iv1 == iv2) {
            return true;
        }
        
        // one of them is null
        if (iv1 == null || iv2 == null) {
            return false;
        }
        
        // only possibility left if that neither are null
        return iv1.equals(iv2);
        
    }
    
    public static int compare(IV iv1, IV iv2) {
        
        // same IV or both null
        if (iv1 == iv2)
            return 0;
        
        // one of them is null
        if (iv1 == null)
            return -1;
        
        if (iv2 == null)
            return 1;
        
        // only possibility left if that neither are null
        return iv1.compareTo(iv2);
        
    }
    
    /**
     * This method attempts to compare two inline internal values based on
     * their numerical representation, which is different from the natural
     * sort ordering of IVs (the natural sort ordering considers the datatype).
     * <p>
     * So for example, if we have two IVs representing 2.1d and 1l, this method
     * will attempt to compare 2.1 to 1 ignoring the datatype. This is useful
     * for SPARQL filters.
     */
    public static int numericalCompare(IV iv1, IV iv2) {

		if (!iv1.isInline())
			throw new IllegalArgumentException("left term is not inline: left="
					+ iv1 + ", right=" + iv2);

		if (!iv2.isInline())
			throw new IllegalArgumentException(
					"right term is not inline: left=" + iv1 + ", right=" + iv2);
        
//        if (!iv1.isInline() || !iv2.isInline())
//            throw new IllegalArgumentException("not inline terms");
        
		if (!iv1.isLiteral())
			throw new IllegalArgumentException(
					"left term is not literal: left=" + iv1 + ", right=" + iv2);

		if (!iv2.isLiteral())
			throw new IllegalArgumentException(
					"right term is not literal: left=" + iv1 + ", right=" + iv2);

//        if (!iv1.isLiteral() || !iv2.isLiteral())
//            throw new IllegalArgumentException("not literals");
        
        final DTE dte1 = iv1.getDTE();
        final DTE dte2 = iv2.getDTE();

        final int numBooleans = 
                (dte1 == DTE.XSDBoolean ? 1 : 0) +
                (dte2 == DTE.XSDBoolean ? 1 : 0);
        
        // we can do two booleans
        if (numBooleans == 1)
            throw new IllegalArgumentException("only one boolean");

        // we can do two signed numerics
        if (numBooleans == 0)
        if (!dte1.isNumeric() || !dte2.isNumeric())
            throw new IllegalArgumentException("not signed numerics");
        
        // we can use the natural ordering if they have the same DTE
        // this will naturally take care of two booleans or two numerics of the
        // same datatype
        if (dte1 == dte2)
            return iv1.compareTo(iv2);
        
        // otherwise we need to try to convert them into comparable numbers
        final AbstractLiteralIV num1 = (AbstractLiteralIV) iv1; 
        final AbstractLiteralIV num2 = (AbstractLiteralIV) iv2; 
        
        // if one's a BigDecimal we should use the BigDecimal comparator for both
        if (dte1 == DTE.XSDDecimal || dte2 == DTE.XSDDecimal) {
            return num1.decimalValue().compareTo(num2.decimalValue());
        }
        
        // same for BigInteger
        if (dte1 == DTE.XSDInteger || dte2 == DTE.XSDInteger) {
            return num1.integerValue().compareTo(num2.integerValue());
        }
        
        // fixed length numerics
        if (dte1.isFloatingPointNumeric() || dte2.isFloatingPointNumeric()) {
            // non-BigDecimal floating points - use doubles
            return Double.compare(num1.doubleValue(), num2.doubleValue());
        } else {
            // non-BigInteger integers - use longs
            final long a = num1.longValue();
            final long b = num2.longValue();
            return a == b ? 0 : a < b ? -1 : 1;
        }
        
    }
    
    public static final IV numericalMath(final IV iv1, final IV iv2, 
    		final MathOp op) {
    	
		if (!iv1.isInline())
			throw new IllegalArgumentException(
					"left term is not inline: left=" + iv1 + ", right=" + iv2);

		if (!iv2.isInline())
			throw new IllegalArgumentException(
					"right term is not inline: left=" + iv1 + ", right=" + iv2);
        
		if (!iv1.isLiteral())
			throw new IllegalArgumentException(
					"left term is not literal: left=" + iv1 + ", right=" + iv2);

		if (!iv2.isLiteral())
			throw new IllegalArgumentException(
					"right term is not literal: left=" + iv1 + ", right=" + iv2);

        final DTE dte1 = iv1.getDTE();
        final DTE dte2 = iv2.getDTE();

        if (!dte1.isNumeric())
			throw new IllegalArgumentException(
					"right term is not numeric: left=" + iv1 + ", right=" + iv2);

        if (!dte2.isNumeric())
			throw new IllegalArgumentException(
					"left term is not numeric: left=" + iv1 + ", right=" + iv2);

        final AbstractLiteralIV num1 = (AbstractLiteralIV) iv1; 
        final AbstractLiteralIV num2 = (AbstractLiteralIV) iv2; 
        
        // if one's a BigDecimal we should use the BigDecimal comparator for both
        if (dte1 == DTE.XSDDecimal || dte2 == DTE.XSDDecimal) {
            return numericalMath(num1.decimalValue(), num2.decimalValue(), op);
        }
        
        // same for BigInteger
        if (dte1 == DTE.XSDInteger || dte2 == DTE.XSDInteger) {
            return numericalMath(num1.integerValue(), num2.integerValue(), op);
        }
        
        // fixed length numerics
        if (dte1.isFloatingPointNumeric() || dte2.isFloatingPointNumeric()) {
            // non-BigDecimal floating points
        	if (dte1 == DTE.XSDFloat && dte2 == DTE.XSDFloat)
        		return numericalMath(num1.floatValue(), num2.floatValue(), op);
        	else
        		return numericalMath(num1.doubleValue(), num2.doubleValue(), op);
        } else {
            // non-BigInteger integers
        	if (dte1 == DTE.XSDInt && dte2 == DTE.XSDInt)
        		return numericalMath(num1.intValue(), num2.intValue(), op);
        	else
        		return numericalMath(num1.longValue(), num2.longValue(), op);
        }
        
    }
    
    private static final IV numericalMath(final BigDecimal left, 
    		final BigDecimal right, final MathOp op) {
    	
    	switch(op) {
    	case PLUS:
    		return new XSDDecimalIV(left.add(right));
    	case MINUS:
    		return new XSDDecimalIV(left.subtract(right));
    	case MULTIPLY:
    		return new XSDDecimalIV(left.multiply(right));
    	case DIVIDE:
    		return new XSDDecimalIV(left.divide(right));
    	case MIN:
    		return new XSDDecimalIV(left.compareTo(right) < 0 ? left : right);
    	case MAX:
    		return new XSDDecimalIV(left.compareTo(right) > 0 ? left : right);
    	default:
    		throw new UnsupportedOperationException();
    	}
    	
    }
    
    private static final IV numericalMath(final BigInteger left, 
    		final BigInteger right, final MathOp op) {
    	
    	switch(op) {
    	case PLUS:
    		return new XSDIntegerIV(left.add(right));
    	case MINUS:
    		return new XSDIntegerIV(left.subtract(right));
    	case MULTIPLY:
    		return new XSDIntegerIV(left.multiply(right));
    	case DIVIDE:
    		return new XSDIntegerIV(left.divide(right));
    	case MIN:
    		return new XSDIntegerIV(left.compareTo(right) < 0 ? left : right);
    	case MAX:
    		return new XSDIntegerIV(left.compareTo(right) > 0 ? left : right);
    	default:
    		throw new UnsupportedOperationException();
    	}
    	
    }
    
    private static final IV numericalMath(final float left, 
    		final float right, final MathOp op) {
    	
    	switch(op) {
    	case PLUS:
    		return new XSDFloatIV(left+right);
    	case MINUS:
    		return new XSDFloatIV(left-right);
    	case MULTIPLY:
    		return new XSDFloatIV(left*right);
    	case DIVIDE:
    		return new XSDFloatIV(left/right);
    	case MIN:
    		return new XSDFloatIV(Math.min(left,right));
    	case MAX:
    		return new XSDFloatIV(Math.max(left,right));
    	default:
    		throw new UnsupportedOperationException();
    	}
    	
    }
    
    private static final IV numericalMath(final double left, 
    		final double right, final MathOp op) {
    	
    	switch(op) {
    	case PLUS:
    		return new XSDDoubleIV(left+right);
    	case MINUS:
    		return new XSDDoubleIV(left-right);
    	case MULTIPLY:
    		return new XSDDoubleIV(left*right);
    	case DIVIDE:
    		return new XSDDoubleIV(left/right);
    	case MIN:
    		return new XSDDoubleIV(Math.min(left,right));
    	case MAX:
    		return new XSDDoubleIV(Math.max(left,right));
    	default:
    		throw new UnsupportedOperationException();
    	}
    	
    }
    
    private static final IV numericalMath(final int left, 
    		final int right, final MathOp op) {
    	
    	switch(op) {
    	case PLUS:
    		return new XSDIntIV(left+right);
    	case MINUS:
    		return new XSDIntIV(left-right);
    	case MULTIPLY:
    		return new XSDIntIV(left*right);
    	case DIVIDE:
    		return new XSDIntIV(left/right);
    	case MIN:
    		return new XSDIntIV(Math.min(left,right));
    	case MAX:
    		return new XSDIntIV(Math.max(left,right));
    	default:
    		throw new UnsupportedOperationException();
    	}
    	
    }
    
    private static final IV numericalMath(final long left, 
    		final long right, final MathOp op) {
    	
    	switch(op) {
    	case PLUS:
    		return new XSDLongIV(left+right);
    	case MINUS:
    		return new XSDLongIV(left-right);
    	case MULTIPLY:
    		return new XSDLongIV(left*right);
    	case DIVIDE:
    		return new XSDLongIV(left/right);
    	case MIN:
    		return new XSDLongIV(Math.min(left,right));
    	case MAX:
    		return new XSDLongIV(Math.max(left,right));
    	default:
    		throw new UnsupportedOperationException();
    	}
    	
    }
    
    /**
     * Used to test whether a given value constant can be used in an inline
     * filter or not.  If so, we can use one of the inline constraints
     * {@link InlineGT}, {@link AbstractInlineConstraint}, {@link LT}, {@link LE}, which in turn defer
     * to the numerical comparison operator {@link #numericalCompare(IV, IV)}. 
     */
    public static final boolean canNumericalCompare(final IV iv) {
        
        // inline boolean or inline signed numeric
        return iv.isInline() && iv.isLiteral() &&
                (iv.getDTE() == DTE.XSDBoolean || iv.isNumeric());
        
    }

    /**
     * Convenience method.  Return true if both operands return true.
     */
    public static final boolean canNumericalCompare(final IV iv1, final IV iv2) {
    	
    	return canNumericalCompare(iv1) && canNumericalCompare(iv2);
    	
    }
    
    /**
     * Encode an RDF value into a key for one of the statement indices.  Handles
     * null {@link IV} references gracefully.
     * 
     * @param keyBuilder
     *            The key builder.
     * @param iv
     *            The internal value (can be <code>null</code>).
     * 
     * @return The key builder.
     */
    public static IKeyBuilder encode(final IKeyBuilder keyBuilder, final IV iv) {

        if (iv == null) {

            NullIV.INSTANCE.encode(keyBuilder);
            
        } else {
            
            iv.encode(keyBuilder);
            
        }

        return keyBuilder;
        
    }
    
    /**
     * Decode an {@link IV} from a byte[].
     * 
     * @param key
     *            The byte[].
     *            
     * @return The {@link IV}.
     */
    public static IV decode(final byte[] key) {

        return decodeFromOffset(key, 0);
        
    }
    
    /**
     * Decodes up to numTerms {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     * @param numTerms
     *            The number of terms to decode.
     *            
     * @return The set of {@link IV}s.
     */
    public static IV[] decode(final byte[] key, final int numTerms) {

    	return decode(key, 0 /* offset */, numTerms);
    	
    }
    
    /**
     * Decodes up to numTerms {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     * @param offset
     *            The offset into the byte[] key.
     * @param numTerms
     *            The number of terms to decode.
     *            
     * @return The set of {@link IV}s.
     */
    public static IV[] decode(final byte[] key, final int offset, 
    		final int numTerms) {
        	
        if (numTerms <= 0)
            return new IV[0];
        
        final IV[] ivs = new IV[numTerms];
        
        int o = offset;
        
        for (int i = 0; i < numTerms; i++) {

            if (o >= key.length)
                throw new IllegalArgumentException(
                        "key is not long enough to decode " 
                        + numTerms + " terms.");
            
            ivs[i] = decodeFromOffset(key, o);
            
            o += ivs[i] == null 
                    ? NullIV.INSTANCE.byteLength() : ivs[i].byteLength();
            
        }

        return ivs;
        
    }
    
    /**
     * Decodes all {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     *            
     * @return The set of {@link IV}s.
     */
    public static IV[] decodeAll(final byte[] key) {

        final ArrayList<IV> ivs = new ArrayList<IV>();
        
        int offset = 0;
        
        while (offset < key.length) {

            final IV iv = decodeFromOffset(key, offset);
            
            ivs.add(iv);
            
            offset += iv == null
                    ? NullIV.INSTANCE.byteLength() : iv.byteLength();
            
        }
        
        return ivs.toArray(new IV[ivs.size()]);
        
    }
    
    private static IV decodeFromOffset(final byte[] key, final int offset) {

        int o = offset;
        
        final byte flags = KeyBuilder.decodeByte(key[o++]);

        /*
         * Handle an IV which is not 100% inline.
         */
        if (!AbstractIV.isInline(flags)) {

            if (AbstractIV.isExtension(flags)) {

                /*
                 * Handle non-inline URI or Literal. 
                 */
                
                // Decode the extension IV.
                final TermId<?> extensionIV = (TermId<?>) IVUtility
                        .decodeFromOffset(key, o);
                
                // skip over the extension IV.
                o += extensionIV.byteLength();

                // Decode the inline component.
                final InlineLiteralIV<BigdataLiteral> delegate = (InlineLiteralIV<BigdataLiteral>) IVUtility
                        .decodeFromOffset(key, o);

                switch (AbstractIV.getInternalValueTypeEnum(flags)) {
                case URI:
                    return new URIWithNamespaceIV<BigdataURI>(delegate, extensionIV);
                case LITERAL:
                    return new LiteralWithDatatypeIV<BigdataLiteral>(delegate, extensionIV);
                default:
                    throw new AssertionError();
                }

            } else {

                /*
                 * Handle a TermId.
                 * 
                 * FIXME TERMS REFACTOR: NullIV.
                 */ 

                // decode the term identifier.
                final long termId = KeyBuilder.decodeLong(key, o);

                if (termId == TermId.NULL)
                    return null;
                else
                    return new TermId(flags, termId);

            }

        }

        /*
         * Handle an inline value. 
         */

        // The value type (URI, Literal, BNode, SID)
        final VTE vte = AbstractIV.getInternalValueTypeEnum(flags);

        switch (vte) {
        case STATEMENT: {
            /*
             * Handle inline sids.
             */
            // spo is directly decodable from key
            final ISPO spo = SPOKeyOrder.SPO.decodeKey(key, o);
            // all spos that have a sid are explicit
            spo.setStatementType(StatementEnum.Explicit);
            spo.setStatementIdentifier(true);
            // create a sid iv and return it
            return new SidIV(spo);
        }
        case BNODE: {
            // The data type
            final DTE dte = AbstractIV.getInternalDataTypeEnum(flags);
            switch (dte) {
            case XSDInt: {
                final int x = KeyBuilder.decodeInt(key, o);
                return new NumericBNodeIV<BigdataBNode>(x);
            }
            case UUID: {
                final UUID x = KeyBuilder.decodeUUID(key, o);
                return new UUIDBNodeIV<BigdataBNode>(x);
            }
            case XSDString: {
                // decode buffer.
                final StringBuilder sb = new StringBuilder();
                // inline string value
                final String str1;
                // #of bytes read.
                final int nbytes;
                try {
                    nbytes = un.decode(new ByteArrayInputStream(key, o,
                            key.length - o), sb);
                    str1 = sb.toString();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return new UnicodeBNodeIV<BigdataBNode>(str1,
                        1/* flags */+ nbytes);
            }
            default:
                throw new AssertionError();
            }
        }
        case URI: {
            // decode buffer.
            final StringBuilder sb = new StringBuilder();
            // inline string value
            final String str1;
            // #of bytes read.
            final int nbytes;
            try {
                nbytes = un.decode(new ByteArrayInputStream(key, o, key.length
                        - o), sb);
                str1 = sb.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new InlineURIIV<BigdataURI>(new URIImpl(str1),
                    1/* flags */+ nbytes);
        }
        case LITERAL:
            break;
        default:
            throw new AssertionError();
        }
        
        /*
         * Inline literal.
         */

        // The data type
        final DTE dte = AbstractIV.getInternalDataTypeEnum(flags);
        
        final boolean isExtension = AbstractIV.isExtension(flags);
        
        final TermId datatype; 
        if (isExtension) {
            datatype = (TermId) decodeFromOffset(key, o);
            o += datatype.byteLength();
        } else {
            datatype = null;
        }

        switch (dte) {
        case XSDBoolean: {
            final byte x = KeyBuilder.decodeByte(key[o]);
            final AbstractLiteralIV iv = (x == 0) ? 
                    XSDBooleanIV.FALSE : XSDBooleanIV.TRUE;
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDByte: {
            final byte x = KeyBuilder.decodeByte(key[o]);
            final AbstractLiteralIV iv = new XSDByteIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDShort: {
            final short x = KeyBuilder.decodeShort(key, o);
            final AbstractLiteralIV iv = new XSDShortIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDInt: {
            final int x = KeyBuilder.decodeInt(key, o);
            final AbstractLiteralIV iv = new XSDIntIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv;
        }
        case XSDLong: {
            final long x = KeyBuilder.decodeLong(key, o);
            final AbstractLiteralIV iv = new XSDLongIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDFloat: {
            final float x = KeyBuilder.decodeFloat(key, o);
            final AbstractLiteralIV iv = new XSDFloatIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDDouble: {
            final double x = KeyBuilder.decodeDouble(key, o);
            final AbstractLiteralIV iv = new XSDDoubleIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDInteger: {
            final BigInteger x = KeyBuilder.decodeBigInteger(o, key);
            final AbstractLiteralIV iv = new XSDIntegerIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDDecimal: {
            final BigDecimal x = KeyBuilder.decodeBigDecimal(o, key);
            final AbstractLiteralIV iv = new XSDDecimalIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case UUID: {
            final UUID x = KeyBuilder.decodeUUID(key, o);
            final AbstractLiteralIV iv = new UUIDLiteralIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv;
        }
            // case XSDUnsignedByte:
            // keyBuilder.appendUnsigned(t.byteValue());
            // break;
            // case XSDUnsignedShort:
            // keyBuilder.appendUnsigned(t.shortValue());
            // break;
            // case XSDUnsignedInt:
            // keyBuilder.appendUnsigned(t.intValue());
            // break;
            // case XSDUnsignedLong:
            // keyBuilder.appendUnsigned(t.longValue());
            // break;
        case XSDString: {
            if(isExtension) {
            // decode the termCode
            final byte termCode = key[o++];
            assert termCode == ITermIndexCodes.TERM_CODE_LIT : "termCode="
                    + termCode;
            // decode buffer.
            final StringBuilder sb = new StringBuilder();
            // inline string value
            final String str1;
            // #of bytes read.
            final int nread;
            try {
                nread = un.decode(new ByteArrayInputStream(key, o, key.length
                        - o), sb);
                str1 = sb.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // Note: The 'delegate' will be an InlineLiteralIV w/o a datatype.
            final InlineLiteralIV<BigdataLiteral> iv = new InlineLiteralIV<BigdataLiteral>(
                    str1, null/* languageCode */, null/* datatype */,
                    1/* flags */+ nread);
            return isExtension ? new ExtensionIV<BigdataLiteral>(iv, datatype)
                    : iv;
            }
            return decodeInlineLiteral(key,o);
        }
        default:
            throw new UnsupportedOperationException("vte=" + vte + ", dte="
                    + dte);
        }

    }
    
    static private InlineLiteralIV<BigdataLiteral> decodeInlineLiteral(
            final byte[] key, final int offset) {

        int o = offset;

        /*
         * Fully inline literal.
         */

        // decode the termCode
        final byte termCode = key[o++];
        // figure out the #of string values which were inlined.
        final int nstrings;
        final String str1, str2; 
        switch (termCode) {
        case ITermIndexCodes.TERM_CODE_LIT:
            nstrings = 1;
            break;
        case ITermIndexCodes.TERM_CODE_LCL:
            nstrings = 2;
            break;
        case ITermIndexCodes.TERM_CODE_DTL:
            nstrings = 2;
            break;
        default:
            throw new AssertionError("termCode=" + termCode);
        }
        // #of bytes read (not including the flags and termCode).
        int nread = 0;
        // decode buffer.
        final StringBuilder sb = new StringBuilder();
        // first inline string value
        try {
            final int nbytes = un.decode(new ByteArrayInputStream(key, o, key.length
                    - o), sb);
            str1 = sb.toString();
            nread += nbytes;
            o += nbytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // second inline string value
        if (nstrings == 2) {
            sb.setLength(0); // reset buffer.
            try {
                final int nbytes = un.decode(new ByteArrayInputStream(key,
                        o, key.length - o), sb);
                str2 = sb.toString();
                nread += nbytes;
                o += nbytes;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            str2 = null;
        }
        final int byteLength = 1/* flags */+ 1/* termCode */+ nread;
        final InlineLiteralIV<BigdataLiteral> iv;
        switch (termCode) {
        case ITermIndexCodes.TERM_CODE_LIT:
            iv = new InlineLiteralIV<BigdataLiteral>(//
                    str1,//
                    null, // language
                    null, // datatype
                    byteLength//
                    );
            break;
        case ITermIndexCodes.TERM_CODE_LCL:
            iv = new InlineLiteralIV<BigdataLiteral>(//
                    str2,//
                    str1, // language
                    null, // datatype
                    byteLength//
                    );
            break;
        case ITermIndexCodes.TERM_CODE_DTL:
            iv = new InlineLiteralIV<BigdataLiteral>(//
                    str2,//
                    null, // language
                    new URIImpl(str1), // datatype
                    byteLength//
                    );
            break;
        default:
            throw new AssertionError("termCode=" + termCode);
        }
        return iv;
    }
    
    /**
     * Decode an IV from its string representation as encoded by
     * {@link TermId#toString()} and 
     * {@link AbstractInlineIV#toString()}.
     * 
     * @param s
     *          the string representation
     * @return
     *          the IV
     */
    public static final IV fromString(final String s) {
        if (s.startsWith("TermId")) { // FIXME TERMS REFACTOR : "TermId" => TermIV?
            final char type = s.charAt(s.length() - 2);
            final long tid = Long.valueOf(s.substring(7, s.length() - 2));
            return new TermId(VTE.valueOf(type), tid);
        } else {
            final String type = s.substring(0, s.indexOf('(')); 
            final String val = s.substring(s.indexOf('('), s.length()-1);
            final DTE dte = Enum.valueOf(DTE.class, type);
            switch (dte) {
            case XSDBoolean: {
                final boolean b = Boolean.valueOf(val);
                if (b) {
                    return XSDBooleanIV.TRUE;
                } else {
                    return XSDBooleanIV.FALSE;
                }
            }
            case XSDByte: {
                final byte x = Byte.valueOf(val);
                return new XSDByteIV<BigdataLiteral>(x);
            }
            case XSDShort: {
                final short x = Short.valueOf(val);
                return new XSDShortIV<BigdataLiteral>(x);
            }
            case XSDInt: {
                final int x = Integer.valueOf(val);
                return new XSDIntIV<BigdataLiteral>(x);
            }
            case XSDLong: {
                final long x = Long.valueOf(val);
                return new XSDLongIV<BigdataLiteral>(x);
            }
            case XSDFloat: {
                final float x = Float.valueOf(val);
                return new XSDFloatIV<BigdataLiteral>(x);
            }
            case XSDDouble: {
                final double x = Double.valueOf(val);
                return new XSDDoubleIV<BigdataLiteral>(x);
            }
            case UUID: {
                final UUID x = UUID.fromString(val);
                return new UUIDLiteralIV<BigdataLiteral>(x);
            }
            case XSDInteger: {
                final BigInteger x = new BigInteger(val);
                return new XSDIntegerIV<BigdataLiteral>(x);
            }
            case XSDDecimal: {
                final BigDecimal x = new BigDecimal(val);
                return new XSDDecimalIV<BigdataLiteral>(x);
            }
                // case XSDUnsignedByte:
                // keyBuilder.appendUnsigned(t.byteValue());
                // break;
                // case XSDUnsignedShort:
                // keyBuilder.appendUnsigned(t.shortValue());
                // break;
                // case XSDUnsignedInt:
                // keyBuilder.appendUnsigned(t.intValue());
                // break;
                // case XSDUnsignedLong:
                // keyBuilder.appendUnsigned(t.longValue());
                // break;
            default:
                throw new UnsupportedOperationException("dte=" + dte);
            }
        }
    }
    
}
