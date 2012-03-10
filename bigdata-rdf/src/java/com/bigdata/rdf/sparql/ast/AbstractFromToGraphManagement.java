/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 10, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;

/**
 * Any of the operations which has a FROM and/or TO graph.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractFromToGraphManagement extends GraphManagement {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public AbstractFromToGraphManagement(UpdateType updateType) {
        super(updateType);
    }

    /**
     * @param op
     */
    public AbstractFromToGraphManagement(AbstractFromToGraphManagement op) {
        super(op);
    }

    /**
     * @param args
     * @param anns
     */
    public AbstractFromToGraphManagement(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
    }

    /**
     * {@inheritDoc}
     * 
     * @return The source graph -or- <code>null</code> if the source is the
     *         "default" graph.
     */
    @Override
    public ConstantNode getSourceGraph() {
        
        return (ConstantNode) getProperty(Annotations.SOURCE_GRAPH);
        
    }

    @Override
    public void setSourceGraph(final ConstantNode sourceGraph) {

        if (sourceGraph == null)
            throw new IllegalArgumentException();
        
        setProperty(Annotations.SOURCE_GRAPH, sourceGraph);
        
    }

    /**
     * {@inheritDoc}
     * 
     * @return The target graph -or- <code>null</code> if the target is the
     *         "default" graph.
     */
    @Override
    final public ConstantNode getTargetGraph() {
        
        return (ConstantNode) getProperty(Annotations.TARGET_GRAPH);
        
    }

    @Override
    final public void setTargetGraph(final ConstantNode targetGraph) {

        if (targetGraph == null)
            throw new IllegalArgumentException();
        
        setProperty(Annotations.TARGET_GRAPH, targetGraph);
        
    }

    /**
     * Return <code>true</code> iff the target is the "default graph".
     */
    final public boolean isTargetDefault() {
        
        return getTargetGraph() == null;
        
    }
    
    /**
     * Return <code>true</code> iff the source is the "default graph".
     */
    final public boolean isSourceDefault() {
        
        return getSourceGraph() == null;
        
    }
    
    //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
    //ADD ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT)
    //MOVE (SILENT)? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT)
    final public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append(indent(indent));
        
        sb.append(getUpdateType());

        if (isSilent())
            sb.append(" SILENT");

        {
            final ConstantNode sourceGraph = getTargetGraph();

            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append(" source=" + sourceGraph == null ? "default"
                    : sourceGraph);
        }

        {
            final ConstantNode targetGraph = getTargetGraph();

            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append("target=" + targetGraph == null ? "default" : targetGraph);
        }

        sb.append("\n");

        return sb.toString();

    }

}
