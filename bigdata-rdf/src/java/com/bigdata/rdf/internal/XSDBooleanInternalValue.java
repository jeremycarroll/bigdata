package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:boolean</code>. */
public class XSDBooleanInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, Boolean> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static public transient final XSDBooleanInternalValue<BigdataLiteral> TRUE = new XSDBooleanInternalValue<BigdataLiteral>(
            true);

    static public transient final XSDBooleanInternalValue<BigdataLiteral> FALSE = new XSDBooleanInternalValue<BigdataLiteral>(
            false);
    
    private final boolean value;

    public XSDBooleanInternalValue(final boolean value) {
        
        super(DTE.XSDBoolean);
        
        this.value = value;
        
    }

    final public Boolean getInlineValue() {

        return value ? Boolean.TRUE : Boolean.FALSE;

    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        final V v = (V) f.createLiteral(value);
        v.setIV(this);
        return v;
    }

    @Override
    final public long longValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean booleanValue() {
        return value;
    }

    @Override
    public byte byteValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double doubleValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float floatValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int intValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short shortValue() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String stringValue() {
        return Boolean.toString(value);
    }

    @Override
    public BigDecimal decimalValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigInteger integerValue() {
        throw new UnsupportedOperationException();
    }

    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof XSDBooleanInternalValue<?>) {
            return this.value == ((XSDBooleanInternalValue<?>) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the byte value.
     * 
     * @see Boolean#hashCode()
     */
    public int hashCode() {
        return value ? Boolean.TRUE.hashCode() : Boolean.FALSE.hashCode();
    }

    public int byteLength() {
        return 1 + 1;
    }

    @Override
    protected int _compareTo(IV o) {
         
        final boolean v = ((XSDBooleanInternalValue) o).value;
        
        return (v == value ? 0 : (value ? 1 : -1));
        
    }
    

}