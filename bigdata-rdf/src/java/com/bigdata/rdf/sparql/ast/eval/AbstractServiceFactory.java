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
 * Created on Sep 9, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;

/**
 * An abstract ServiceFactory that deals with service parameters (magic
 * predicates that connfigure the service).
 */
public abstract class AbstractServiceFactory implements ServiceFactory {

    private static final Logger log = Logger
            .getLogger(AbstractServiceFactory.class);

    /**
     * The service parameters.  Can be multi-valued.  Map from predicate to
     * one or more TermNode values.
     */
    public static class ServiceParams {

    	/**
    	 * The map of service params.
    	 */
    	final Map<URI, List<TermNode>> params;
    	
    	public ServiceParams() {
    		
    		this.params = new LinkedHashMap<URI, List<TermNode>>();
    		
    	}
    	
    	/**
    	 * Add.
    	 */
    	public void add(final URI param, final TermNode value) {
    		
    		if (!params.containsKey(param)) {
    			
    			params.put(param, new LinkedList<TermNode>());
    			
    		}
    		
    		params.get(param).add(value);
    		
    	}
    	
    	/**
    	 * Set (clear and add).
    	 */
    	public void set(final URI param, final TermNode value) {
    		
    		clear(param);
    		
    		add(param, value);
    		
    	}
    	
    	/**
    	 * Clear.
    	 */
    	public void clear(final URI param) {
    		
    		params.remove(param);
    		
    	}

    	/**
    	 * Check for existence.
    	 */
    	public boolean contains(final URI param) {
    		
    		return params.containsKey(param);
    		
    	}

    	/**
    	 * Get a singleton value for the specified param.
    	 */
    	public TermNode get(final URI param, final TermNode defaultValue) {
    		
    		if (params.containsKey(param)) {
    			
    			final List<TermNode> values = params.get(param);
    			
    			if (values.size() > 1) {
    				
    				throw new RuntimeException("not a singleton param");
    				
    			}
    			
    			return values.get(0);
    			
    		}
    		
    		return defaultValue;
    		
    	}
    	
    	/**
    	 * Helper.
    	 */
    	public Boolean getAsBoolean(final URI param) {
    		
    		return getAsBoolean(param, null);
    		
    	}
    		
    	/**
    	 * Helper.
    	 */
    	public Boolean getAsBoolean(final URI param, final Boolean defaultValue) {
    		
    		final Literal term = getAsLiteral(param, null);
    		
    		if (term != null) {
    			
    			return term.booleanValue();
    			
    		}
    		
    		return defaultValue;
    		
    	}
    	
    	/**
    	 * Helper.
    	 */
    	public Integer getAsInt(final URI param) {
    		
    		return getAsInt(param, null);
    		
    	}
    		
    	/**
    	 * Helper.
    	 */
    	public Integer getAsInt(final URI param, final Integer defaultValue) {
    		
    		final Literal term = getAsLiteral(param, null);
    		
    		if (term != null) {
    			
    			return term.intValue();
    			
    		}
    		
    		return defaultValue;
    		
    	}
    	
    	/**
    	 * Helper.
    	 */
    	public Long getAsLong(final URI param) {
    		
    		return getAsLong(param, null);
    		
    	}
    		
    	/**
    	 * Helper.
    	 */
    	public Long getAsLong(final URI param, final Long defaultValue) {
    		
    		final Literal term = getAsLiteral(param, null);
    		
    		if (term != null) {
    			
    			return term.longValue();
    			
    		}
    		
    		return defaultValue;
    		
    	}
    	
    	/**
    	 * Helper.
    	 */
    	public String getAsString(final URI param) {
    		
    		return getAsString(param, null);
    		
    	}
    		
    	/**
    	 * Helper.
    	 */
    	public String getAsString(final URI param, final String defaultValue) {
    		
    		final Literal term = getAsLiteral(param, null);
    		
    		if (term != null) {
    			
    			return term.stringValue();
    			
    		}
    		
    		return defaultValue;
    		
    	}
    	
    	/**
    	 * Helper.
    	 */
    	public Literal getAsLiteral(final URI param) {
    		
    		return getAsLiteral(param, null);
    		
    	}
    		
    	/**
    	 * Helper.
    	 */
    	public Literal getAsLiteral(final URI param, final Literal defaultValue) {
    		
    		final TermNode term = get(param, null);
    		
    		if (term != null) {
    			
    			if (term.isVariable()) {
    				
    				throw new IllegalArgumentException("not a constant");
    				
    			}
    			
    			final Value v = term.getValue();
    			
    			if (!(v instanceof Literal)) {
    				
    				throw new IllegalArgumentException("not a literal");
    				
    			}
    			
    			return ((Literal) v);
    			
    		}
    		
    		return defaultValue;
    		
    	}

    	/**
    	 * Helper.
    	 */
    	public URI getAsURI(final URI param) {
    		
    		return getAsURI(param, null);
    		
    	}
    		
    	/**
    	 * Helper.
    	 */
    	public URI getAsURI(final URI param, final URI defaultValue) {
    		
    		final TermNode term = get(param, null);
    		
    		if (term != null) {
    			
    			if (term.isVariable()) {
    				
    				throw new IllegalArgumentException("not a constant");
    				
    			}
    			
    			final Value v = term.getValue();
    			
    			if (!(v instanceof URI)) {
    				
    				throw new IllegalArgumentException("not a uri");
    				
    			}
    			
    			return ((URI) v);
    			
    		}
    		
    		return defaultValue;
    		
    	}

    	/**
    	 * Helper.
    	 */
    	public IVariable<IV> getAsVar(final URI param) {
    		
    		return getAsVar(param, null);
    		
    	}
    	
    	/**
    	 * Helper.
    	 */
    	public IVariable<IV> getAsVar(final URI param, final IVariable<IV> defaultValue) {
    		
    		final TermNode term = get(param, null);
    		
    		if (term != null) {
    			
    			if (!term.isVariable()) {
    				
    				throw new IllegalArgumentException("not a var");
    				
    			}
    			
    			return (IVariable<IV>) term.getValueExpression();
    			
    		}
    		
    		return defaultValue;
    		
    	}

    	/**
    	 * Helper.
    	 */
    	public List<TermNode> get(final URI param) {
    		
    		if (params.containsKey(param)) {
    			
    			return params.get(param);
    			
    		}
    		
    		return Collections.EMPTY_LIST;
    		
    	}
    	
    	/**
    	 * Iterator.
    	 */
    	public Iterator<Map.Entry<URI, List<TermNode>>> iterator() {
    		
    		return params.entrySet().iterator();
    		
    	}
    	
    	public String toString() {
    		
    		final StringBuilder sb = new StringBuilder();
    		
			sb.append("[");
			
    		for (Map.Entry<URI, List<TermNode>> e : params.entrySet()) {
    			
    			final URI param = e.getKey();
    			
    			final List<TermNode> terms = e.getValue();
    			
    			sb.append(param).append(": ");
    			
    			if (terms.size() == 1) {
    				
    				sb.append(terms.get(0));
    				
    			} else {
    				
    				sb.append("[");
    				for (TermNode t : terms) {
    					
    					sb.append(t).append(", ");
    					
    				}
    				sb.setLength(sb.length()-2);
    				sb.append("]");
    				
    			}
    			
    			sb.append(", ");
    			
    		}
    		
    		if (sb.length() > 1)
    			sb.setLength(sb.length()-2);
			sb.append("]");
    		
    		return sb.toString();
    		
    	}
    	
    }
    
    public AbstractServiceFactory() {
    }
    
    /**
     * Create a {@link BigdataServiceCall}.  Does the work of collecting
     * the service parameter triples and then delegates to 
     * {@link #create(ServiceCallCreateParams, ServiceParams)}.
     */
    public BigdataServiceCall create(final ServiceCallCreateParams params) {

        if (params == null)
            throw new IllegalArgumentException();

        final AbstractTripleStore store = params.getTripleStore();

        if (store == null)
            throw new IllegalArgumentException();

        final ServiceNode serviceNode = params.getServiceNode();

        if (serviceNode == null)
            throw new IllegalArgumentException();

        final ServiceParams serviceParams = gatherServiceParams(params);
        
		if (log.isDebugEnabled()) {
			log.debug(serviceParams);
		}
		
        return create(params, serviceParams);
        
    }
    
    /**
     * Implemented by subclasses - verify the group and create the service call.
     */
    public abstract BigdataServiceCall create(
    		final ServiceCallCreateParams params,
    		final ServiceParams serviceParams);

    /**
     * Gather the service params (any statement patterns with the subject
     * of {@link BD#SERVICE_PARAM}.
     */
    protected ServiceParams gatherServiceParams(
    		final ServiceCallCreateParams createParams) {
    	
        if (createParams == null)
            throw new IllegalArgumentException();

        final AbstractTripleStore store = createParams.getTripleStore();

        if (store == null)
            throw new IllegalArgumentException();

        final ServiceNode serviceNode = createParams.getServiceNode();

        if (serviceNode == null)
            throw new IllegalArgumentException();

        final GraphPatternGroup<IGroupMemberNode> group = 
        		serviceNode.getGraphPattern();
        
        if (group == null)
            throw new IllegalArgumentException();
        
        final ServiceParams serviceParams = new ServiceParams();
        
        final Iterator<IGroupMemberNode> it = group.iterator();
        
        while (it.hasNext()) {
        
        	final IGroupMemberNode node = it.next();
        	
        	if (node instanceof StatementPatternNode) {
        		
        		final StatementPatternNode sp = (StatementPatternNode) node;
        		
        		final TermNode s = sp.s();
        		
        		if (s.isConstant() && BD.SERVICE_PARAM.equals(s.getValue())) {
        			
        			if (sp.p().isVariable()) {
        				
        				throw new RuntimeException(
        						"not a valid service param triple pattern, " +
        						"predicate must be constant: " + sp);
        				
        			}
        			
        			final URI param = (URI) sp.p().getValue();
        			
        			serviceParams.add(param, sp.o());
        			
        		}
        		
        	}
        	
        }
            	
        return serviceParams;
    	
    }

}
