package com.bigdata.rdf.spo;

import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;

import org.openrdf.model.URI;

import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * "IN" filter for the context position based on a native long hash set
 * containing the acceptable graph identifiers. While evaluation of the
 * access path will be ordered, the filter does not maintain evolving state
 * so a hash set will likely beat a binary search.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 * 
 * @see InGraphBinarySearchFilter
 * 
 * @todo tighten serialization?
 */
public final class InGraphHashSetFilter extends SPOFilter {

    /**
     * 
     */
    private static final long serialVersionUID = -6059009162692785772L;

    final LongLinkedOpenHashSet contextSet;
    
    /**
     * 
     * @param graphs
     *            The set of acceptable graph identifiers.
     */
    public InGraphHashSetFilter(final Iterable<? extends URI> graphs) {

        /*
         * Create a sorted array of term identifiers for the set of contexts
         * we will accept.
         */

        contextSet = new LongLinkedOpenHashSet();
        
        for (URI uri : graphs) {
        
            final long termId = ((BigdataURI) uri).getTermId();
            
            if (termId != IRawTripleStore.NULL) {

                contextSet.add(termId);
                
            }
            
        }
        
    }

    public boolean accept(final ISPO spo) {
        
        return contextSet.contains(spo.c());
        
    }

}