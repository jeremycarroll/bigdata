package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * @deprecated by the AST refactor. 
 */
public class BigdataConstructIterator implements
        CloseableIteration<Statement, QueryEvaluationException> {
    private final BigdataStatementIterator stmtIt;
    
    /**
     * The leftovers are for bindingSets that cannot be resolved into
     * bigdata statements because they contain values not in the lexicon.
     */
    private final Collection<Statement> leftovers;
    
    private Iterator<Statement> leftoversIt;
    
    private final ValueFactory vf;

    private boolean open = true;

    public BigdataConstructIterator(
            final AbstractTripleStore db,
            final CloseableIteration<? extends BindingSet, QueryEvaluationException> src,
            final ValueFactory vf) {
        assert db != null && src != null;
        this.vf = vf;
        this.leftovers = new LinkedList<Statement>();
        /*
         * Note: CONSTRUCT only scopes bnodes (or at least bnodes in the
         * template) to the solution.
         */
        stmtIt = db.asStatementIterator(db
                .bulkCompleteStatements(new ChunkedWrappedIterator<ISPO>(
                        new SPOConverter(src))));
    }

    public boolean hasNext() throws QueryEvaluationException {
        
        if(open && _hasNext())
            return true;
        
        close();
        
        return false;
        
    }

    private boolean _hasNext() throws QueryEvaluationException {
        if (stmtIt.hasNext()) {
            return true;
        }
        // nothing left in the primary iterator, time for leftovers
        if (leftoversIt == null) {
            leftoversIt = leftovers.iterator();
        }
        return leftoversIt.hasNext();
    }

    public Statement next() throws QueryEvaluationException {
        if (stmtIt.hasNext()) {
            return stmtIt.next();
        }
        // nothing left in the primary iterator, time for leftovers
        if (leftoversIt == null) {
            leftoversIt = leftovers.iterator();
        }
        return leftoversIt.next();
    }

    public void remove() throws QueryEvaluationException {
        throw new UnsupportedOperationException();
    }

    public void close() throws QueryEvaluationException {
        
        if(open) {
         
            open = false;
            
            stmtIt.close();
            
        }
        
    }
    
    /**
     * This bindingSet contains a value that is not in the lexicon.
     * 
     * @param bindingSet
     */
    private void addToLeftovers(final BindingSet bindingSet) {
        
        final Resource subject = (Resource)bindingSet.getValue("subject");
        final URI predicate = (URI)bindingSet.getValue("predicate");
        final Value object = bindingSet.getValue("object");
        final Resource context = (Resource)bindingSet.getValue("context");
        if (context == null) {
            leftovers.add(vf.createStatement(subject, predicate, object));
        }
        else {
            leftovers.add(vf.createStatement(subject, predicate, object, context));
        }
        
    }

    private class SPOConverter implements ICloseableIterator<ISPO> {
        
        private final CloseableIteration<? extends BindingSet, QueryEvaluationException> src;
        
        private SPO next;

        private boolean open = true;
        
        public SPOConverter(
                final CloseableIteration<? extends BindingSet, QueryEvaluationException> src) {
            
            this.src = src;
            
        }
        
        public void close() {
            if (open) {
                open = false;
                try {
                    src.close();
                } catch (QueryEvaluationException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        /**
         * Determine if there is another valid SPO in the bindingSet iteration.
         * If the bindingSet contains a value that is not in the lexicon, we
         * need to handle that separately (without trying to resolve an SPO).
         */
        public boolean hasNext() {

            if (open && _hasNext())
                return true;
            
            close();
            
            return false;
            
        }

        private boolean _hasNext() {
            try {
                // we already have our next lined up
                if (next != null) {
                    return true;
                }
                // we don't have one on deck, and there isn't one left in the iterator
                if (!src.hasNext()) {
                    return false;
                }
                // pluck the next one out of the iterator
                final BindingSet bs = src.next();
                next = convert(bs);
                if (isValid(next)) {
                    // if we can convert it we're good to go
                    return true;
                } else {
                    // otherwise add it to the leftovers and try again
                    addToLeftovers(bs);
                    next = null;
                    return hasNext();
                }
            } catch (QueryEvaluationException ex) {
                throw new RuntimeException(ex);
            }
        }

        public SPO next() {
            // getting the next is actually handled in hasNext()
            if(!hasNext())
                throw new NoSuchElementException();
            // clear out the next so we can get a new one
            final SPO spo = next;
            next = null;
            return spo;
        }

        public void remove() {
            if(!open)
                throw new IllegalStateException();
            try {
                src.remove();
            } catch (QueryEvaluationException ex) {
                throw new RuntimeException(ex);
            }
        }

        /**
         * Make sure all of the three positions are non-null - i.e. the terms
         * actually exist in the lexicon (not always the case with construct).
         */
        private boolean isValid(final SPO spo) {
            return spo.s != null &&
                   spo.p != null &&
                   spo.o != null;
        }
        
        /**
         * Convert a bindingset into an SPO.  All values should already be
         * bigdata values, we don't use db.getTermId(Value).
         */
        private SPO convert(final BindingSet bindingSet) {
            final Value subject = bindingSet.getValue("subject");
            final Value predicate = bindingSet.getValue("predicate");
            final Value object = bindingSet.getValue("object");
            IV s = null; 
            if (subject instanceof BigdataValue) {
                s = ((BigdataValue) subject).getIV();
            }
            IV p = null; 
            if (predicate instanceof BigdataValue) {
                p = ((BigdataValue) predicate).getIV();
            }
            IV o = null; 
            if (object instanceof BigdataValue) {
                o = ((BigdataValue) object).getIV();
            }
            final SPO spo = new SPO(s, p, o);
            return spo;
        }
        
    }
}
