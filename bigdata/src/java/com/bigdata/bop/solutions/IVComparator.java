/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/* Portions Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on Aug 8, 2011
 */

package com.bigdata.bop.solutions;

import info.aduna.lang.ObjectUtil;

import java.util.Comparator;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * A comparator that compares {@link IV}s according the SPARQL value ordering as
 * specified in <A
 * href="http://www.w3.org/TR/rdf-sparql-query/#modOrderBy">SPARQL Query
 * Language for RDF</a>. This implementation is based on the openrdf
 * {@link ValueComparator} but has been modified to work with {@link IV}s.
 * 
 * @author james
 * @author Arjohn Kampman
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class IVComparator implements Comparator<IV> {

    public int compare(final IV o1, final IV o2) {

        // check equality
        if (ObjectUtil.nullEquals(o1, o2)) {
            return 0;
        }

        // 1. (Lowest) no value assigned to the variable
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }

        // 2. Blank nodes
        final boolean b1 = o1.isBNode();
        final boolean b2 = o2.isBNode();
        if (b1 && b2) {
            return 0;
        }
        if (b1) {
            return -1;
        }
        if (b2) {
            return 1;
        }

        // 3. IRIs
        final boolean u1 = o1.isURI();
        final boolean u2 = o2.isURI();
        if (u1 && u2) {
            return compareURIs((URI) o1, (URI) o2);
        }
        if (u1) {
            return -1;
        }
        if (u2) {
            return 1;
        }

        // 4. RDF literals
        return compareLiterals((IV<BigdataLiteral, ?>) o1,
                (IV<BigdataLiteral, ?>) o2);
    
    }

    /**
     * Only difference here with Sesame ValueComparator is that we use
     * stringValue() instead of toString().
     */
	private int compareURIs(URI leftURI, URI rightURI) {
		return leftURI.stringValue().compareTo(rightURI.stringValue());
	}

	private int compareLiterals(
			final IV<BigdataLiteral, ?> left, final IV<BigdataLiteral, ?> right) {
		
		/*
		 * Only thing we need to special case are LiteralExtensionIVs, which
		 * are used to model xsd:dateTime.
		 */
    	if (left instanceof LiteralExtensionIV &&
    			right instanceof LiteralExtensionIV) {
    	
    		final IV leftDatatype = ((LiteralExtensionIV) left).getExtensionIV();
    		
    		final IV rightDatatype = ((LiteralExtensionIV) right).getExtensionIV();
    		
    		if (leftDatatype.equals(rightDatatype)) {
    		
    			return left.compareTo(right);
    			
    		}
    		
    	}
    	
    	return compareLiterals((Literal) left, (Literal) right);
		
	}
	
	/**
	 * Taken directly from Sesame's ValueComparator, no modification.  Handles
	 * inlines nicely since they now implement the Literal interface.
	 */
	private int compareLiterals(Literal leftLit, Literal rightLit) {
		// Additional constraint for ORDER BY: "A plain literal is lower
		// than an RDF literal with type xsd:string of the same lexical
		// form."

		if (!QueryEvaluationUtil.isStringLiteral(leftLit) || !QueryEvaluationUtil.isStringLiteral(rightLit)) {
			try {
				boolean isSmaller = QueryEvaluationUtil.compareLiterals(leftLit, rightLit, CompareOp.LT);

				if (isSmaller) {
					return -1;
				}
				else {
					return 1;
				}
			}
			catch (ValueExprEvaluationException e) {
				// literals cannot be compared using the '<' operator, continue
				// below
			}
		}

		int result = 0;

		// Sort by datatype first, plain literals come before datatyped literals
		URI leftDatatype = leftLit.getDatatype();
		URI rightDatatype = rightLit.getDatatype();

		if (leftDatatype != null) {
			if (rightDatatype != null) {
				// Both literals have datatypes
				result = compareDatatypes(leftDatatype, rightDatatype);
			}
			else {
				result = 1;
			}
		}
		else if (rightDatatype != null) {
			result = -1;
		}

		if (result == 0) {
			// datatypes are equal or both literals are untyped; sort by language
			// tags, simple literals come before literals with language tags
			String leftLanguage = leftLit.getLanguage();
			String rightLanguage = rightLit.getLanguage();

			if (leftLanguage != null) {
				if (rightLanguage != null) {
					result = leftLanguage.compareTo(rightLanguage);
				}
				else {
					result = 1;
				}
			}
			else if (rightLanguage != null) {
				result = -1;
			}
		}

		if (result == 0) {
			// Literals are equal as fas as their datatypes and language tags are
			// concerned, compare their labels
			result = leftLit.getLabel().compareTo(rightLit.getLabel());
		}

		return result;
	}

	/**
	 * Taken directly from Sesame's ValueComparator, no modification.
	 */
	private int compareDatatypes(URI leftDatatype, URI rightDatatype) {
		if (XMLDatatypeUtil.isNumericDatatype(leftDatatype)) {
			if (XMLDatatypeUtil.isNumericDatatype(rightDatatype)) {
				// both are numeric datatypes
				return compareURIs(leftDatatype, rightDatatype);
			}
			else {
				return -1;
			}
		}
		else if (XMLDatatypeUtil.isNumericDatatype(rightDatatype)) {
			return 1;
		}
		else if (XMLDatatypeUtil.isCalendarDatatype(leftDatatype)) {
			if (XMLDatatypeUtil.isCalendarDatatype(rightDatatype)) {
				// both are calendar datatypes
				return compareURIs(leftDatatype, rightDatatype);
			}
			else {
				return -1;
			}
		}
		else if (XMLDatatypeUtil.isCalendarDatatype(rightDatatype)) {
			return 1;
		}
		else {
			// incompatible or unordered datatypes
			return compareURIs(leftDatatype, rightDatatype);
		}
	}
	
}