package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Aligns bigdata {@link ICloseableIterator} {@link IBindingSet}s containing
 * {@link BigdataValue}s with a Sesame 2 {@link CloseableIteration} visiting
 * Sesame 2 {@link BindingSet}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the thrown exception.
 */
public class Bigdata2Sesame2BindingSetIterator<E extends Exception> implements
        CloseableIteration<BindingSet, E> {

    final protected static Logger log = Logger
            .getLogger(Bigdata2Sesame2BindingSetIterator.class);

    /**
     * The source iterator (will be closed when this iterator is closed).
     */
    private final ICloseableIterator<IBindingSet> src;
    
    private final BindingSet constants;
    
    private boolean open = true;

    /**
     * 
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed). All bound values in the visited {@link IBindingSet}s
     *            MUST be {@link BigdataValue}s.
     */
    public Bigdata2Sesame2BindingSetIterator(final ICloseableIterator<IBindingSet> src) {

        this(src, null);
    }
        
    public Bigdata2Sesame2BindingSetIterator(final ICloseableIterator<IBindingSet> src, final BindingSet constants) {
        
        if (src == null)
            throw new IllegalArgumentException();

        this.src = src;
        
        this.constants = constants;

    }
    
    public boolean hasNext() throws E {

        if(open && src.hasNext())
            return true;
        
        close();
        
        return false;
        
    }

    public BindingSet next() throws E {

        if (!hasNext())
            throw new NoSuchElementException();

        return getBindingSet(src.next());

    }

    /**
     * Aligns a bigdata {@link IBindingSet} with the Sesame 2 {@link BindingSet}.
     * 
     * @param src
     *            A bigdata {@link IBindingSet} containing only
     *            {@link BigdataValue}s.
     * 
     * @return The corresponding Sesame 2 {@link BindingSet}.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws ClassCastException
     *             if a bound value is not a {@link BigdataValue}.
     */
    protected BindingSet getBindingSet(final IBindingSet src) {

        if (src == null)
            throw new IllegalArgumentException();

        final int n = src.size();

        final MapBindingSet bindingSet = new MapBindingSet(n /* capacity */);

        final Iterator<Map.Entry<IVariable,IConstant>> itr = src.iterator();

        while(itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> entry = itr.next();

            bindingSet.addBinding(entry.getKey().getName(),
                    (BigdataValue) entry.getValue().get());
            
        }
        
        if (constants != null) {
            
            final Iterator<Binding> it = constants.iterator();
            while (it.hasNext()) {
                final Binding b = it.next();
                bindingSet.addBinding(b.getName(), b.getValue());
//                bindingSet.addBinding(b);
            }
            
        }
        
        return bindingSet;
        
    }
    
    /**
     * @throws UnsupportedOperationException
     */
    public void remove() throws E {

        throw new UnsupportedOperationException();

    }

    public void close() throws E {

        if(open) {

            open = false;
            
            src.close();
            
        }
        
    }

}
