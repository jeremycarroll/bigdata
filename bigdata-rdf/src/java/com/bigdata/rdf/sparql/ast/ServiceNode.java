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
 * Created on Aug 18, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * An extension point for external service calls which produce solution
 * multisets (a SPARQL <code>SERVICE</code>).
 */
public class ServiceNode extends GroupMemberNodeBase<IGroupMemberNode>
        implements IJoinNode, IGraphPatternContainer {

    private static final long serialVersionUID = 1L;

    interface Annotations extends GroupMemberNodeBase.Annotations,
            IJoinNode.Annotations, IGraphPatternContainer.Annotations {

        /**
         * The {@link TermNode} for the SERVICE URI (either a simple variable or
         * a simple constant).
         */
        String SERVICE_REF = "serviceRef";

        /**
         * The namespace of the {@link AbstractTripleStore} instance (not the
         * namespace of the lexicon relation). This resource will be located and
         * made available to the {@link ServiceCall}.
         */
        String NAMESPACE = "namespace";
     
        /**
         * The "SELECT" option.
         * 
         * TODO Lift out. This is used for many things in SPARQL UPDATE, not 
         * just for SPARQL Federation.
         */
        String SILENT = "silent";
        
        boolean DEFAULT_SILENT = false;
        
        /**
         * The text "image" of the original SPARQL SERVICE clause. The "image"
         * of the original graph pattern is what gets sent to a remote SPARQL
         * end point when we evaluate the SERVICE node. Because the original
         * "image" of the graph pattern is being used, we also need to have the
         * prefix declarations so we can generate a valid SPARQL request.
         */
        String EXPR_IMAGE = "exprImage";
        
        /**
         * The prefix declarations for the SPARQL query from which the
         * {@link #EXPR_IMAGE} was taken. This is needed in order to generate a
         * valid SPARQL query for a remote SPARQL end point when we evaluate the
         * SERVICE request.
         */
        String PREFIX_DECLS = "prefixDecls";
        
    }

    /**
     * Required deep copy constructor.
     */
    public ServiceNode(final ServiceNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public ServiceNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * Construct a function node in the AST.
     * 
     * @param serviceRef
     *            The value expression for the SERVICE URI.
     * @param groupNode
     *            The graph pattern used to invoke the service.
     *            
     * @see ServiceRegistry
     */
    public ServiceNode(
            final TermNode serviceRef,
            final GraphPatternGroup<IGroupMemberNode> groupNode) {

        super(new BOp[] {}, null/* anns */);

        setServiceRef(serviceRef);

        setGraphPattern(groupNode);

    }

    /**
     * The service reference.
     */
    public TermNode getServiceRef() {

        return (TermNode) getRequiredProperty(Annotations.SERVICE_REF);

    }

    /**
     * Set the service reference.
     */
    public void setServiceRef(final TermNode serviceRef) {

        if(serviceRef == null)
            throw new IllegalArgumentException();
        
        setProperty(Annotations.SERVICE_REF, serviceRef);

    }

    @SuppressWarnings("unchecked")
    public GraphPatternGroup<IGroupMemberNode> getGraphPattern() {

        return (GraphPatternGroup<IGroupMemberNode>) getRequiredProperty(Annotations.GRAPH_PATTERN);

    }

    @Override
    public void setGraphPattern(
            final GraphPatternGroup<IGroupMemberNode> graphPattern) {

        if (graphPattern == null)
            throw new IllegalArgumentException();
         
        /*
         * Clear the parent reference on the new where clause.
         * 
         * Note: This handles cases where a join group is lifted into a named
         * subquery. If we do not clear the parent reference on the lifted join
         * group it will still point back to its parent in the original join
         * group.
         */
        graphPattern.setParent(null);

        super.setProperty(Annotations.GRAPH_PATTERN, graphPattern);

    }

    /**
     * Returns <code>false</code>.
     */
    final public boolean isOptional() {
     
        return false;
        
    }

    /**
     * Returns <code>false</code>.
     */
    final public boolean isMinus() {
     
        return false;
        
    }

    final public boolean isSilent() {

        return getProperty(Annotations.SILENT, Annotations.DEFAULT_SILENT);

    }

    final public void setSilent(final boolean silent) {

        setProperty(Annotations.SILENT, silent);

    }

    public String getExprImage() {

        return (String) getProperty(Annotations.EXPR_IMAGE);

    }

    /**
     * Set the text "image" of the SPARQL SERVICE clause. This will be used IFF
     * we generate a SPARQL query for a remote SPARQL end point. You must also
     * specify the prefix declarations for that text "image".
     */
    public void setExprImage(final String serviceExpressionString) {

        setProperty(Annotations.EXPR_IMAGE, serviceExpressionString);

    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getPrefixDecls() {

        return (Map) getProperty(Annotations.PREFIX_DECLS);

    }

    /**
     * Set the prefix declarations for the group graph pattern. This will be
     * used IFF we generate a SPARQL query for a remote SPARQL end point. You
     * must also specify the text "image".
     */
    public void setPrefixDecls(final Map<String, String> prefixDecls) {

        setProperty(Annotations.PREFIX_DECLS, prefixDecls);

    }

    final public List<FilterNode> getAttachedJoinFilters() {

        @SuppressWarnings("unchecked")
        final List<FilterNode> filters = (List<FilterNode>) getProperty(Annotations.FILTERS);

        if (filters == null) {

            return Collections.emptyList();

        }

        return Collections.unmodifiableList(filters);

    }

    final public void setAttachedJoinFilters(final List<FilterNode> filters) {

        setProperty(Annotations.FILTERS, filters);

    }

    @Override
    public String toString(int indent) {

        final StringBuilder sb = new StringBuilder();

        final TermNode serviceRef = getServiceRef();

        sb.append("\n");
        sb.append(indent(indent));
        sb.append("SERVICE");
        if(isSilent()) {
            sb.append(" SILENT");
        }
        if(serviceRef.isConstant()) {
            sb.append(" <");
            sb.append(serviceRef);
            sb.append(">");
        } else {
            sb.append(" ?");
            sb.append(serviceRef);
        }

        if (getGraphPattern() != null) {

            sb.append(" {");
            
            sb.append(getGraphPattern().toString(indent+1));
            
            sb.append("\n").append(indent(indent)).append("}");
            
        }
        
        final List<FilterNode> filters = getAttachedJoinFilters();
        if(!filters.isEmpty()) {
            for (FilterNode filter : filters) {
                sb.append(filter.toString(indent + 1));
            }
        }

        if (getQueryHints() != null && !getQueryHints().isEmpty()) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append(Annotations.QUERY_HINTS);
            sb.append("=");
            sb.append(getQueryHints().toString());
        }
        
        return sb.toString();

    }

}
