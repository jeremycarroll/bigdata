package com.bigdata.rdf.model;

import java.util.Comparator;

/**
 * Places {@link BigdataValue}s into an ordering determined by their assigned
 * {@link BigdataValue#getTermId() term identifiers}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BigdataValue#getTermId()
 */
public class TermIdComparator implements Comparator<BigdataValue> {

    public static final transient Comparator<BigdataValue> INSTANCE =
        new TermIdComparator();

    /**
     * Note: comparison avoids possible overflow of <code>long</code> by
     * not computing the difference directly.
     */
    public int compare(BigdataValue term1, BigdataValue term2) {

        final long id1 = term1.getTermId();
        final long id2 = term2.getTermId();
        
        if(id1 < id2) return -1;
        if(id1 > id2) return 1;
        return 0;

    }

}