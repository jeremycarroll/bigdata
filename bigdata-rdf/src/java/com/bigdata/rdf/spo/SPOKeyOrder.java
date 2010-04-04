/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.spo;

import java.io.Serializable;
import java.util.Comparator;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.striterator.IKeyOrder;

/**
 * Represents the key order used by an index for a triple relation.
 * 
 * @serial The serialization of the class is quite small since the only instance
 *         field is {@link #index()}. All other data are static. However, it is
 *         still MUCH more efficient to only transmit the {@link #index()} byte
 *         without the overhead of the class metadata, which is an additional
 *         <strong>60</strong> bytes! Classes embedding serialized
 *         {@link SPOKeyOrder} are strongly encouraged to make this
 *         optimization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum SPOKeyOrder implements IKeyOrder<ISPO>, Serializable {
	//Triple KeyOrders
	SPO( 3, new int[] { ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.O }),//
	OSP( 3, new int[] { ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.P }),//
	POS( 3, new int[] { ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.S }),//
	//Standard Quad KeyOrders
	SPOC( 4, new int[] { ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.C }),//
	POCS( 4, new int[] { ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.S }),//
	OCSP( 4, new int[] { ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.P }),//
	CSPO( 4, new int[] { ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.O }),//
	PCSO( 4, new int[] { ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.O }),//
	SOPC( 4, new int[] { ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.C }),//
	
	//All Extra Quad KeyOrders which can be used in subclasses
	SPCO( 4, new int[] { ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.O }),//
	SOCP( 4, new int[] { ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.P }),//
	SCOP( 4, new int[] { ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.P }), // 
	SCPO( 4, new int[] { ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.O }), // 
	
	POSC( 4, new int[] { ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.C }),//
	PCOS( 4, new int[] { ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.S }),//
	PSCO( 4, new int[] { ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.O }),//
	PSOC( 4, new int[] { ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.C }),//
	
	OCPS( 4, new int[] { ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.S }),//
	OSPC( 4, new int[] { ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.C }),//
	OSCP( 4, new int[] { ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.P }),//
	OPSC( 4, new int[] { ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.C }),//
	OPCS( 4, new int[] { ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.S }),//
	
	CSOP( 4, new int[] { ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.P }),//
	CPOS( 4, new int[] { ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.S }),//
	CPSO( 4, new int[] { ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.O }),//
	COSP( 4, new int[] { ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.S, ISPOKeyOrderProvider.P }),//
	COPS( 4, new int[] { ISPOKeyOrderProvider.C, ISPOKeyOrderProvider.O, ISPOKeyOrderProvider.P, ISPOKeyOrderProvider.S });//
	

	
	private static final long serialVersionUID = 87501920529732159L;

	static private final transient long NULL = IRawTripleStore.NULL;
	
	/**
	 * The unique index used to identify this natural order.
	 */
	
	final int arity;
	final int[] order;
	final Comparator<ISPO> comparator;
	SPOKeyOrder(int arity,int[] order) {
		this.arity=arity;
		this.order=order;
		this.comparator=new GeneralComparator(order);
	}



	public String getIndexName() {
		return name();
	}

	/**
	 * Return either 3 or 4 depending on the #of components in the key for this
	 * natural key ordering.
	 */
	final public int getKeyArity() {
		return arity;
	}

	/**
	 * Return the index of the slot in the {@link ISPO} tuple which appears at
	 * the specified position in the key.
	 * 
	 * @param keyPos
	 *            The index into the key that is being generated.
	 * 
	 * @return The index of the slot in the {@link ISPO}.
	 */
	final public int getKeyOrder(final int keyPos) {

		return order[keyPos];

	}


	/**
	 * Return the comparator that places {@link ISPO}s into the natural order
	 * for the associated index.
	 * 
	 * @todo performance comparison if we get rid of the {@link SPOComparator},
	 *       {@link POSComparator}, and {@link OSPComparator} and just use the
	 *       {@link GeneralComparator}. Will the hot spot compiler do better
	 *       with just one class handling all of those comparisons?
	 */
	final public Comparator<ISPO> getComparator() {
		return comparator;
	}

	
	/**
	 * Return the inclusive lower bound which would be used for a query against
	 * this {@link IKeyOrder} for the given {@link IPredicate}.
	 * 
	 * @todo This method should be declared by {@link IKeyOrder}.
	 */
	final public byte[] getFromKey(final IKeyBuilder keyBuilder,
			final IPredicate<ISPO> predicate) {

		final int keyArity = getKeyArity(); // use the key's "arity".

		keyBuilder.reset();

		boolean noneBound = true;

		for (int i = 0; i < keyArity; i++) {

			final IVariableOrConstant<Long> term = predicate.get(getKeyOrder(i));

			final long l;

			// Note: term MAY be null for the context position.
			if (term == null || term.isVar()) {

				l = Long.MIN_VALUE;

			} else {

				l = term.get();

				noneBound = false;

			}

			keyBuilder.append(l);

		}

		return noneBound ? null : keyBuilder.getKey();

	}

	/**
	 * Return the exclusive upper bound which would be used for a query against
	 * this {@link IKeyOrder} for the given {@link IPredicate}.
	 * 
	 * @todo This method should be declared by {@link IKeyOrder}.
	 */
	final public byte[] getToKey(final IKeyBuilder keyBuilder,
			final IPredicate<ISPO> predicate) {

		keyBuilder.reset();

		final int keyArity = getKeyArity();

		boolean noneBound = true;

		boolean foundLastBound = false;

		for (int i = 0; i < keyArity; i++) {

			final IVariableOrConstant<Long> term = predicate
					.get(getKeyOrder(i));

			long l;

			// Note: term MAY be null for context.
			if (term == null || term.isVar()) {

				l = Long.MIN_VALUE;

			} else {

				l = term.get();

				noneBound = false;

				if (!foundLastBound) {

					if (i == keyArity - 1) {

						l++;

						foundLastBound = true;

					} else {

						final IVariableOrConstant<Long> next = predicate
								.get(getKeyOrder(i + 1));

						// Note: next can be null for quads (context pos).
						if (next == null || next.isVar()) {

							l++;

							foundLastBound = true;

						}

					}

				}

			}

			keyBuilder.append(l);

		}

		return noneBound ? null : keyBuilder.getKey();

	}

	final public byte[] encodeKey(final IKeyBuilder keyBuilder, final ISPO spo) {

		keyBuilder.reset();


		for (int i = 0; i < order.length; i++) {

			keyBuilder.append(spo.get(order[i]));

		}

		return keyBuilder.getKey();

	}

	/**
	 * Decode the key into an {@link SPO}. The {@link StatementEnum} and the
	 * optional SID will not be decoded, since it is carried in the B+Tree
	 * value. However, if the {@link SPOKeyOrder} is a quad order then the
	 * {@link SPO#c()} will be bound.
	 * 
	 * @param keyOrder
	 *            The natural order of the key.
	 * @param key
	 *            The key.
	 * 
	 * @return The decoded key.
	 */
	final public SPO decodeKey(final byte[] key) {

		/*
		 * Note: GTE since the key is typically a reused buffer which may be
		 * larger than the #of bytes actually holding valid data.
		 */
		final int keyArity = getKeyArity();

		assert key.length >= 8 * keyArity;

		/*
		 * Re-order the key into SPO order.
		 */

		final long[] val=new long[4];

		for(int  i=0;i<4;i++) {
			val[order[i]]=(i<3||arity==4)? KeyBuilder.decodeLong(key, i*8): IRawTripleStore.NULL;
		}

		return new SPO(val[0],val[1],val[2],val[3]);

	}
	/**
	 * Generalized comparator for {@link ISPO}s.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id$
	 */
	private static class GeneralComparator implements Comparator<ISPO> {

		private final int keyMap[];

		public GeneralComparator(int keyMap[]) {

			this.keyMap = keyMap;

		}

		public int compare(final ISPO o1, final ISPO o2) {

			if (o1 == o2) {
				return 0;
			}

			// compare terms one by one in the appropriate key order
			for (int i = 0; i < keyMap.length; i++) {

				final long t1 = o1.get(keyMap[i]);

				final long t2 = o2.get(keyMap[i]);

				int ret = t1 < t2 ? -1 : t1 > t2 ? 1 : 0;

				if (ret != 0) {

					return ret;

				}

			}

			// all terms match
			return 0;

		}

	}
}
