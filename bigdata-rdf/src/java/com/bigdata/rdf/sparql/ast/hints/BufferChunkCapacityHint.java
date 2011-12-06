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
 * Created on Nov 27, 2011
 */

package com.bigdata.rdf.sparql.ast.hints;

import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Sets the capacity of the {@link IBindingSet}[]s used to accumulate a chunk of
 * {@link IBindingSet}s (default {@value #DEFAULT_CHUNK_CAPACITY}). Partial
 * chunks may be automatically combined into full chunks.
 * 
 * @see BufferAnnotations#CHUNK_CAPACITY
 */
final class BufferChunkCapacityHint extends AbstractIntQueryHint {

    protected BufferChunkCapacityHint() {
        super(BufferAnnotations.CHUNK_CAPACITY,
                BufferAnnotations.DEFAULT_CHUNK_CAPACITY);
    }

    @Override
    public void handle(final AST2BOpContext context,
            final QueryHintScope scope, final ASTBase op, final Integer value) {

        if (op instanceof StatementPatternNode) {

            /*
             * Note: This is set on the queryHint Properties object and then
             * transferred to the pipeline operator when it is generated.
             */

            _setQueryHint(context, scope, op, getName(), value);

        }

    }

}