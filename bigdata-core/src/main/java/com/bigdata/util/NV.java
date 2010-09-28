package com.bigdata.util;

import java.io.Serializable;

/**
 * A name-value pair.
 * 
 * Note: this class has a natural ordering that is inconsistent with equals.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NV implements Serializable, Comparable<NV> {
    
    /**
     * 
     */
    private static final long serialVersionUID = -6919300153058191480L;

    private final String name;
    
    private final String value;
    
    public String getName() {
        
        return name;
        
    }
    
    public String getValue() {
        
        return value;
        
    }

    public String toString() {
        
        return name + "=" + value;
        
    }
    
    /**
     * 
     * @throws IllegalArgumentException
     *             if the <i>name</i> is <code>null</code>.
     */
    public NV(final String name, final String value) {

        if (name == null)
            throw new IllegalArgumentException();

//        if (value == null)
//            throw new IllegalArgumentException();

        this.name = name;

        this.value = value;
        
    }
    
    @Override
    public int hashCode() {        
        return name.hashCode();        
    }
    
    @Override
    public boolean equals(Object o) {
    	if (this==o) return true;
    	
    	if (!(o instanceof NV)) {
    		return false;
    	}
        NV nv = (NV)o;
        
        return name.equals(nv.name) && value.equals(nv.value);
        
    }

    /**
     * Places into order by <code>name</code>.
     */
    public int compareTo(NV o) {
        
        int ret = name.compareTo(o.name);
        
        return ret;
        
    }
    
//    /**
//     * Combines the two arrays, appending the contents of the 2nd array to the
//     * contents of the first array.
//     * 
//     * @param a
//     * @param b
//     * @return
//     */
//    public static NV[] concat(final NV[] a, final NV[] b) {
//
//        if (a == null && b == null)
//            return a;
//
//        if (a == null)
//            return b;
//
//        if (b == null)
//            return a;
//
//        final NV[] c = (NV[]) java.lang.reflect.Array.newInstance(a.getClass()
//                .getComponentType(), a.length + b.length);
//
//        System.arraycopy(a, 0, c, 0, a.length);
//
//        System.arraycopy(b, 0, c, a.length, b.length);
//
//        return c;
//
//    }

}
