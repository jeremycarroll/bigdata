package com.bigdata.rdf.spo;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueImpl;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariableOrConstant;

/**
 * @author Matthew Roy ( <a
 *         href="mailto:mroy@cambridgesemantics.com">mroy@cambridgesemantics.
 *         com</a>)
 * 
 */
public class SPOKeyOrderProvider implements ISPOKeyOrderProvider {

	/**
     * Canonicalizing mapping for {@link SPOKeyOrderProvider}s based on the
     * namespace of the {@link SPOKeyOrderProvider}.
     */
    private static WeakValueCache<String/* namespace */, SPOKeyOrderProvider> cache = new WeakValueCache<String, SPOKeyOrderProvider>(
            new LRUCache<String, SPOKeyOrderProvider>(1/* capacity */));

    /**
     * Return the instance associated with the <i>namespace</i>.
        * @param namespace
     *            The namespace of the {@link SPOKeyOrderProvider}.
     */
    public static SPOKeyOrderProvider getInstance(final String namespace) {
        
        if (namespace == null)
            throw new IllegalArgumentException();
        
        synchronized(cache) {
            
        	SPOKeyOrderProvider a = cache.get(namespace);

            if (a == null) {

                a = new SPOKeyOrderProvider();

                cache.put(namespace, a, true/* dirty */);
                
            }
            
            return a;
            
        }
        
    }
    
    /**
     * Remove a {@link SPOKeyOrderProvider} from the canonicalizing mapping.
     * @param namespace
     *            The namespace of the {@link SPOKeyOrderProvider}.
     */
    public void remove(final String namespace) {
        
        if (namespace == null)
            throw new IllegalArgumentException();
        
        synchronized(cache) {
        
            cache.remove(namespace);
            
        }
        
    }
	    
	/*
	 * Constants corresponding to the columns of the SPO(C) relation.
	 */
	protected final transient long NULL = IRawTripleStore.NULL;

	final public SPOKeyOrder tripleStoreIndices[];
	
	final public SPOKeyOrder quadStoreIndices[];

	/**
	 * 
	 */
	public SPOKeyOrderProvider(SPOKeyOrder tripleStoreIndices[],SPOKeyOrder quadStoreIndices[]) {
		this.tripleStoreIndices =tripleStoreIndices;
		this.quadStoreIndices=quadStoreIndices;
	}
	
	public SPOKeyOrderProvider() {	
		this(new SPOKeyOrder[] {
					SPOKeyOrder.SPO,SPOKeyOrder.OSP,SPOKeyOrder.POS}
			,new SPOKeyOrder[] {
				SPOKeyOrder.SPOC,SPOKeyOrder.POCS,SPOKeyOrder.OCSP,SPOKeyOrder.CSPO,SPOKeyOrder.PCSO,SPOKeyOrder.SOPC}
		);
	}
	
	public int index(SPOKeyOrder keyOrder) {
		return keyOrder.ordinal();
	}
	
	/**
	 * Return the {@link SPOKeyOrder} for the given predicate.
	 * 
	 * @param predicate
	 *            The predicate.
	 * 
	 * @return The {@link SPOKeyOrder}
	 * 
	 * @todo A variant of this method should be raised onto IKeyOrder without
	 *       the keyArity parameter. That parameter is only there because we
	 *       support two distinct families of natural orders in this class: one
	 *       for triples and one for quads.
	 */
	public SPOKeyOrder getKeyOrder(final IPredicate<ISPO> predicate,
			final int keyArity) {

		final long s = predicate.get(0).isVar() ? NULL : (Long) predicate
				.get(0).get();

		final long p = predicate.get(1).isVar() ? NULL : (Long) predicate
				.get(1).get();

		final long o = predicate.get(2).isVar() ? NULL : (Long) predicate
				.get(2).get();

		if (keyArity == 3) {

			// Note: Context is ignored!

			if (s != NULL && p != NULL && o != NULL) {
				return SPOKeyOrder.SPO;
			} else if (s != NULL && p != NULL) {
				return SPOKeyOrder.SPO;
			} else if (s != NULL && o != NULL) {
				return SPOKeyOrder.OSP;
			} else if (p != NULL && o != NULL) {
				return SPOKeyOrder.POS;
			} else if (s != NULL) {
				return SPOKeyOrder.SPO;
			} else if (p != NULL) {
				return SPOKeyOrder.POS;
			} else if (o != NULL) {
				return SPOKeyOrder.OSP;
			} else {
				return SPOKeyOrder.SPO;
			}

		} else {

			@SuppressWarnings("unchecked")
			final IVariableOrConstant<Long> t = predicate.get(3);

			final long c = t == null ? NULL : (t.isVar() ? NULL : t.get());

			/*
			 * if ((s == NULL && p == NULL && o == NULL && c == NULL) || (s !=
			 * NULL && p == NULL && o == NULL && c == NULL) || (s != NULL && p
			 * != NULL && o == NULL && c == NULL) || (s != NULL && p != NULL &&
			 * o != NULL && c == NULL) || (s != NULL && p != NULL && o != NULL
			 * && c != NULL)) { return SPOC; }
			 */

			if ((s == NULL && p != NULL && o == NULL && c == NULL)
					|| (s == NULL && p != NULL && o != NULL && c == NULL)
					|| (s == NULL && p != NULL && o != NULL && c != NULL)) {
				return SPOKeyOrder.POCS;
			}

			if ((s == NULL && p == NULL && o != NULL && c == NULL)
					|| (s == NULL && p == NULL && o != NULL && c != NULL)
					|| (s != NULL && p == NULL && o != NULL && c != NULL)) {
				return SPOKeyOrder.OCSP;
			}

			if ((s == NULL && p == NULL && o == NULL && c != NULL)
					|| (s != NULL && p == NULL && o == NULL && c != NULL)
					|| (s != NULL && p != NULL && o == NULL && c != NULL)) {
				return SPOKeyOrder.CSPO;
			}

			if ((s == NULL && p != NULL && o == NULL && c != NULL)) {
				return SPOKeyOrder.PCSO;
			}

			if ((s != NULL && p == NULL && o != NULL && c == NULL)) {
				return SPOKeyOrder.SOPC;
			}

			return SPOKeyOrder.SPOC;

		}

	}

	public SPOKeyOrder[] getTripleStoreIndices() {
		return tripleStoreIndices;
	}

	public SPOKeyOrder[] getQuadStoreIndices() {
		return quadStoreIndices;
	}

	public SPOKeyOrder getPredicateFirstKeyOrder(boolean quads) {
		return quads ? SPOKeyOrder.POCS : SPOKeyOrder.POS;
	}

	public SPOKeyOrder getSubjectFirstKeyOrder(boolean quads) {
		return quads ? SPOKeyOrder.SCOP : SPOKeyOrder.SPO;
	}

	public SPOKeyOrder getObjectFirstKeyOrder(boolean quads) {
		return quads ? SPOKeyOrder.OCSP : SPOKeyOrder.OSP;
	}

	public SPOKeyOrder getContextFirstKeyOrder() {
		return SPOKeyOrder.CSPO;
	}

	public SPOKeyOrder getPrimaryTripleStoreIndex() {
		return SPOKeyOrder.SPO;
	}

	public SPOKeyOrder getPrimaryQuadStoreIndex() {
		return SPOKeyOrder.SPOC;
	}

	/**
	 * Return an iterator which visits the triple store indices (
	 * {@link SPOKeyOrder#SPO}, {@link SPOKeyOrder#POS}, {@link SPOKeyOrder#OSP}
	 * ).
	 */
	public Iterator<SPOKeyOrder> tripleStoreKeyOrderIterator() {

		return Arrays.asList(tripleStoreIndices).iterator();

	}

	/**
	 * Return an iterator which visits the quad store indices (
	 * {@link SPOKeyOrder#SPOC}, {@link SPOKeyOrder#POCS},
	 * {@link SPOKeyOrder#OCSP}, {@link SPOKeyOrder#CSPO},
	 * {@link SPOKeyOrder#PCSO}, {@link SPOKeyOrder#SOPC} ).
	 */
	public Iterator<SPOKeyOrder> quadStoreKeyOrderIterator() {

		return Arrays.asList(quadStoreIndices).iterator();

	}

	/**
	 * Return an iterator which visits only {@link SPOKeyOrder#SPO}.
	 */
	public Iterator<SPOKeyOrder> spoOnlyKeyOrderIterator() {

		return new SPOOnlyKeyOrderIterator();

	}

	/**
	 * Return an iterator which visits only {@link SPOKeyOrder#SPOC}.
	 */
	public Iterator<SPOKeyOrder> spocOnlyKeyOrderIterator() {

		return new SPOCOnlyKeyOrderIterator();

	}

	/**
	 * Iterator visits {@link #SPO}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id$
	 */
	private class SPOOnlyKeyOrderIterator implements
				Iterator<SPOKeyOrder> {

		boolean exhausted = false;

		public boolean hasNext() {
			return !exhausted;
		}

		public SPOKeyOrder next() {
			if (!hasNext())
				throw new NoSuchElementException();
			exhausted = true;
			return SPOKeyOrder.SPO;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	/**
	 * Iterator visits {@link #SPOC}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id$
	 */
	private class SPOCOnlyKeyOrderIterator implements
				Iterator<SPOKeyOrder> {

		boolean exhausted = false;

		public boolean hasNext() {
			return !exhausted;
		}

		public SPOKeyOrder next() {
			if (!hasNext())
				throw new NoSuchElementException();
			exhausted = true;
			return SPOKeyOrder.SPOC;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

	}
}
