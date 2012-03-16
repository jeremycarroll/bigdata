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

import com.bigdata.rdf.internal.impl.AbstractInlineExtensionIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Configuration determines which RDF Values are inlined into the statement
 * indices rather than being assigned term identifiers by the lexicon.
 */
public interface ILexiconConfiguration<V extends BigdataValue> {

    /**
     * Create an inline {@link IV} for the supplied RDF value if inlining is
     * supported for the supplied RDF value.
     * <p>
     * Note: If the supplied RDF value is a {@link BigdataValue} then <em>the
     * {@link IV} will be set as a side-effect</em> and will be available from
     * {@link BigdataValue#getIV()}.
     * 
     * @param value
     *            The RDF value
     * 
     * @return The inline {@link IV} -or- <code>null</code> if the {@link Value}
     *         can not be represented by an {@link IV}.
     */
    IV createInlineIV(final Value value);
    
    /**
     * Create an RDF value from an {@link AbstractInlineExtensionIV}. The
     * "extension" {@link IV} MUST be registered with the {@link Vocabulary}.
     * <p>
     * For {@link LiteralExtensionIV}, this through an internal catalog of
     * {@link IExtension}s to find one that knows how to handle the extension
     * datatype from the supplied {@link LiteralExtensionIV}. This is the
     * historical use case.
     * 
     * @param iv
     *            the extension IV
     * 
     * @return The RDF {@link Value}
     */
    V asValue(final LiteralExtensionIV<?> iv);

    /**
     * Return the {@link Value} for that {@link IV} iff the {@link IV} is
     * declared in the {@link Vocabulary}.
     * 
     * @param iv
     *            The {@link IV}.
     *            
     * @return The {@link Value} -or- <code>null</code> if the {@link IV} was
     *         not declared in the {@link Vocabulary}.
     */
    V asValueFromVocab(final IV<?,?> iv);
    
    /**
     * Initialize the extensions, which need to resolve their datatype URIs into
     * term ids.
     */
    void initExtensions(final LexiconRelation lex);

    /**
     * Return the value factory for the lexicon.
     */
    BigdataValueFactory getValueFactory();

}
