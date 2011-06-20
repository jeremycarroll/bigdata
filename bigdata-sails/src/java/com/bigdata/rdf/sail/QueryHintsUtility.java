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
/*
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) � 2001-2007

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
/*
 * Created on Jun 20, 2011
 */

package com.bigdata.rdf.sail;

import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;

import com.bigdata.rdf.sail.sparql.BaseDeclProcessor;
import com.bigdata.rdf.sail.sparql.PrefixDeclProcessor;
import com.bigdata.rdf.sail.sparql.StringEscapesProcessor;

/**
 * A utility class for parsing {@link QueryHints}.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class QueryHintsUtility {

    /**
     * Parse query hints from a query string. Query hints are embedded in the
     * query string via special namespaces.
     * <p>
     * Note: The Sesame operator tree does not include the original query hints,
     * which is why this method is not written against the operator tree.
     * 
     * See {@link QueryHints#PREFIX} for more information.
     */
    public static Properties parseQueryHints(final QueryLanguage ql,
            final String queryString, final String baseURI)
            throws MalformedQueryException {
        try {
            final Properties queryHints = new Properties();
            // currently only supporting SPARQL
            if (ql == QueryLanguage.SPARQL) {
                // the next four lines were taken directly from
                // org.openrdf.query.parser.sparql.SPARQLParser.parseQuery(String queryStr, String baseURI)
                final ASTQueryContainer qc = SyntaxTreeBuilder
                        .parseQuery(queryString);
                StringEscapesProcessor.process(qc);
                BaseDeclProcessor.process(qc, baseURI);
                final Map<String, String> prefixes = PrefixDeclProcessor
                        .process(qc);
                // iterate the namespaces
                for (Map.Entry<String, String> prefix : prefixes.entrySet()) {
                    // if we see one that matches the magic namespace, try
                    // to parse it
                    if (prefix.getKey().equalsIgnoreCase(QueryHints.PREFIX)) {
                        String hints = prefix.getValue();
                        // has to have a # and it can't be at the end
                        int i = hints.indexOf('#');
                        if (i < 0 || i == hints.length() - 1) {
                            throw new MalformedQueryException(
                                    "bad query hints: " + hints);
                        }
                        hints = hints.substring(i + 1);
                        // properties are separated by &
                        final StringTokenizer st = new StringTokenizer(hints,
                                "&");
                        while (st.hasMoreTokens()) {
                            final String hint = st.nextToken();
                            i = hint.indexOf('=');
                            if (i < 0 || i == hint.length() - 1) {
                                throw new MalformedQueryException(
                                        "bad query hint: " + hint);
                            }
                            final String key = hint.substring(0, i);
                            final String val = hint.substring(i+1);
                            queryHints.put(key, val);
                        }
                    }
                }
            }
            return queryHints;
        } catch (ParseException e) {
            throw new MalformedQueryException(e.getMessage(), e);
        }
    }
    
}
