package com.bigdata.relation.rule.eval;

import com.bigdata.relation.accesspath.IElementFilter;

/**
 * Resolves an {@link ISolution} to its element and delegates the filter
 * test to an {@link IElementFilter} suitable for the expected element type.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SolutionFilter<E> implements IElementFilter<ISolution<E>> {

    /**
     * 
     */
    private static final long serialVersionUID = 6747357650593183644L;
    
    private final IElementFilter<E> delegate;

    public SolutionFilter(IElementFilter<E> delegate) {
        
        if (delegate == null)
            throw new IllegalArgumentException();
        
        this.delegate = delegate;
        
    }
    
    public boolean accept(ISolution<E> solution) {

        final E e = solution.get();
        
        return delegate.accept( e );
    }
    
    public boolean canAccept(Class c) {

        return delegate.canAccept( c );
        
    }
 
    public String toString() {
        
        return getClass().getSimpleName() + "{delegate=" + delegate + "}";
        
    }
    
}
