/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.rdf.internal;

import org.openrdf.model.Value;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Configuration determines which RDF Values are inlined into the statement
 * indices rather than being assigned term identifiers by the lexicon.
 */
public interface ILexiconConfiguration<V extends BigdataValue> {

	/**
	 * Create an inline {@link IV} for the supplied RDF value if inlining is
	 * supported for the supplied RDF value.
	 * <p>
	 * If the supplied RDF value is a {@link BigdataValue} then the {@link IV}
	 * will be set as a side-effect.
	 * 
	 * @param value
	 *            the RDF value
	 *            
	 * @return The inline {@link IV} -or- <code>null</code> if we the
	 *         {@link Value} can not be represented by an {@link IV}.
	 */
    IV createInlineIV(final Value value);
    
    /**
     * Create an RDF value from an {@link ExtensionIV}. Looks through an
     * internal catalog of {@link IExtension}s to find one that knows how to
     * handle the extension datatype from the supplied {@link ExtensionIV}.
     * 
     * @param iv
     *          the extension IV
     * @param vf
     *          the bigdata value factory
     * @return
     *          the RDF value
     */
    V asValue(final ExtensionIV iv, final BigdataValueFactory vf);
    
    /**
     * Initialize the extensions, which need to resolve their datatype URIs
     * into term ids.
     */
    void initExtensions(final LexiconRelation lex);
    
}
