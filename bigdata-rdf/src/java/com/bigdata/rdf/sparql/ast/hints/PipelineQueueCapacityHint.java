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

import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.QueryHintScope;

/**
 * For pipelined operators, this is the capacity of the input queue for that
 * operator. Producers will block if the input queue for the target operator is
 * at its capacity. This provides an important limit on the amount of data which
 * can be buffered on the JVM heap during pipelined query evaluation.
 * 
 * @see PipelineOp.Annotations#PIPELINE_QUEUE_CAPACITY
 */
final class PipelineQueueCapacityHint extends AbstractIntQueryHint {

    protected PipelineQueueCapacityHint() {
        super(PipelineOp.Annotations.PIPELINE_QUEUE_CAPACITY,
                PipelineOp.Annotations.DEFAULT_PIPELINE_QUEUE_CAPACITY);
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