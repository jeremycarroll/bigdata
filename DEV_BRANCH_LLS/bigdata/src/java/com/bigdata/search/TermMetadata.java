package com.bigdata.search;

import java.util.ArrayList;

import org.apache.lucene.analysis.Token;

/**
 * Mutable metadata for the occurrences of a term within a field of some
 * document.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TermMetadata {
    
    public String termText() {
        
        return occurrences.get(0);
        
    }
    
    /**
     * The term frequency count.
     */
    public int termFreq() {
        
        return occurrences.size();
        
    }
    
    /**
     * The local term weight, which may be computed by a variety of methods.
     */
    public double localTermWeight;

    // @todo make private?
    ArrayList<String> occurrences = new ArrayList<String>();
    
    /**
     * Add an occurrence.
     * 
     * @param token
     *            The token.
     */
    public void add(String token) {

        assert token != null;
        
        occurrences.add(token);
        
    }
    
}