package com.bigdata.rdf.spo;

import java.util.Iterator;

import com.bigdata.relation.rule.IPredicate;

/**
 * @author Matthew Roy ( <a href="mailto:mroy@cambridgesemantics.com">mroy@cambridgesemantics.com</a>)
 *
 */
public interface ISPOKeyOrderProvider {
	public abstract void remove(String namespace);
	
	final static int S = 0;
	final static int P = 1;
	final static int O = 2;
	final static int C = 3;
	
	public abstract int index(SPOKeyOrder keyOrder);
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
	public abstract SPOKeyOrder getKeyOrder(final IPredicate<ISPO> predicate,
			final int keyArity);

	public abstract SPOKeyOrder[] getTripleStoreIndices();

	public abstract SPOKeyOrder[] getQuadStoreIndices();

	public abstract SPOKeyOrder getPredicateFirstKeyOrder(boolean quads);

	public abstract SPOKeyOrder getSubjectFirstKeyOrder(boolean quads);

	public abstract SPOKeyOrder getObjectFirstKeyOrder(boolean quads);

	public abstract SPOKeyOrder getContextFirstKeyOrder();

	public abstract SPOKeyOrder getPrimaryTripleStoreIndex();

	public abstract SPOKeyOrder getPrimaryQuadStoreIndex();

	/**
	 * Return an iterator which visits the triple store indices (
	 * {@link SPOKeyOrder#SPO}, {@link SPOKeyOrder#POS}, {@link SPOKeyOrder#OSP}
	 * ).
	 */
	public abstract Iterator<SPOKeyOrder> tripleStoreKeyOrderIterator();

	/**
	 * Return an iterator which visits the quad store indices (
	 * {@link SPOKeyOrder#SPOC}, {@link SPOKeyOrder#POCS},
	 * {@link SPOKeyOrder#OCSP}, {@link SPOKeyOrder#CSPO},
	 * {@link SPOKeyOrder#PCSO}, {@link SPOKeyOrder#SOPC} ).
	 */
	public abstract Iterator<SPOKeyOrder> quadStoreKeyOrderIterator();

	/**
	 * Return an iterator which visits only {@link SPOKeyOrder#SPO}.
	 */
	public abstract Iterator<SPOKeyOrder> spoOnlyKeyOrderIterator();

	/**
	 * Return an iterator which visits only {@link SPOKeyOrder#SPOC}.
	 */
	public abstract Iterator<SPOKeyOrder> spocOnlyKeyOrderIterator();

}