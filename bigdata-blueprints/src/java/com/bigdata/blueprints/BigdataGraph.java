/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.blueprints;

import info.aduna.iteration.CloseableIteration;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 * A base class for a Blueprints wrapper around a bigdata back-end.
 * 
 * @author mikepersonick
 *
 */
public abstract class BigdataGraph implements Graph {

    private static final transient Logger log = Logger.getLogger(BigdataGraph.class);
    
    public interface Options {
        
        /**
         * Allow multiple edges with the same edge id.  Useful for assigning
         * by-reference properties (e.g. vertex type).
         */
        String LAX_EDGES = BigdataGraph.class.getName() + ".laxEdges";
        
        /**
         * This is necessary to pass the test suites.
         */
        String READ_FROM_WRITE_CONNECTION = BigdataGraph.class.getName() + ".readFromWriteConnection";
        
        /**
         * Use an append model for properties (rather than replace).
         */
        String LAX_PROPERTIES = BigdataGraph.class.getName() + ".laxProperties";
        
    }
    
    /**
     * URI used for typing elements.
     */
    protected final URI TYPE;
    
    /**
     * URI used to represent a Vertex.
     */
    protected final URI VERTEX;
    
    /**
     * URI used to represent a Edge.
     */
    protected final URI EDGE;

    /**
     * URI used for labeling edges.
     */
    protected final URI LABEL;

	/**
	 * Factory for round-tripping between Blueprints data and RDF data.
	 */
	final BlueprintsValueFactory factory;
	
	/**
	 * Allow re-use of edge identifiers.
	 */
	private final boolean laxEdges;
	
	/**
	 * If true, read from the write connection.  Necessary for the test suites.
	 */
	private final boolean readFromWriteConnection;
	
    /**
     * If true, use pure append mode (don't check old property values).
     */
    private final boolean laxProperties;
    
    public BigdataGraph(final BlueprintsValueFactory factory) {
        this(factory, new Properties());
    }
    
	public BigdataGraph(final BlueprintsValueFactory factory,
	        final Properties props) {

	    this.factory = factory;
	    
	    this.laxEdges = Boolean.valueOf(props.getProperty(
	            Options.LAX_EDGES, "false"));
        this.readFromWriteConnection = Boolean.valueOf(props.getProperty(
                Options.READ_FROM_WRITE_CONNECTION, "false"));
        this.laxProperties = Boolean.valueOf(props.getProperty(
                Options.LAX_PROPERTIES, "false"));
	    
	    this.TYPE = factory.getTypeURI();
	    this.VERTEX = factory.getVertexURI();
	    this.EDGE = factory.getEdgeURI();
	    this.LABEL = factory.getLabelURI();
	    
	}
	
	/**
	 * For some reason this is part of the specification (i.e. part of the
	 * Blueprints test suite).
	 */
	public String toString() {
	    
	    return getClass().getSimpleName().toLowerCase();
	    
	}
	
	/**
	 * Return the factory used to round-trip between Blueprints values and
	 * RDF values.
	 */
	public BlueprintsValueFactory getValueFactory() {
	    return factory;
	}
	
    /**
     * Different implementations will return different types of connections
     * depending on the mode (client/server, embedded, read-only, etc.)
     */
	protected abstract RepositoryConnection getWriteConnection() throws Exception;
	
	/**
	 * A read-only connection can be used for read operations without blocking
	 * or being blocked by writers.
	 */
    protected abstract RepositoryConnection getReadConnection() throws Exception;
	
	/**
	 * Return a single-valued property for an edge or vertex.
     * 
     * @see {@link BigdataElement}
	 */
    public Object getProperty(final URI uri, final String prop) {
        
        return getProperty(uri, factory.toPropertyURI(prop));
        
    }

    /**
     * Return a single-valued property for an edge or vertex.
     * 
     * @see {@link BigdataElement}
     */
	public Object getProperty(final URI uri, final URI prop) {

		try {
			
		    final RepositoryConnection cxn = readFromWriteConnection ? 
		            getWriteConnection() : getReadConnection();
		    
			final RepositoryResult<Statement> result = 
					cxn.getStatements(uri, prop, null, false);
			
			if (result.hasNext()) {
				
			    final Statement stmt = result.next();
			    
				if (!result.hasNext()) {

				    /*
				     * Single value.
				     */
				    return getProperty(stmt.getObject());
				    
				} else {

				    /*
				     * Multi-value, use a list.
				     */
				    final List<Object> list = new LinkedList<Object>();

				    list.add(getProperty(stmt.getObject()));
				    
				    while (result.hasNext()) {
				        
				        list.add(getProperty(result.next().getObject()));
				        
				    }
				    
				    return list;
				    
				}

			}
			
			return null;
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}
	
	protected Object getProperty(final Value value) {
	    
        if (!(value instanceof Literal)) {
            throw new RuntimeException("not a property: " + value);
        }
    
        final Literal lit = (Literal) value;
        
        final Object o = factory.fromLiteral(lit);
        
        return o;

	}
	
//    /**
//     * Return a multi-valued property for an edge or vertex.
//     * 
//     * TODO get rid of me
//     * 
//     * @see {@link BigdataElement}
//     */
//    public List<Object> getProperties(final URI uri, final String prop) {
//        
//        return getProperties(uri, factory.toPropertyURI(prop));
//        
//    }
//
//	/**
//     * Return a multi-valued property for an edge or vertex.
//     * 
//     * TODO get rid of me
//     * 
//     * @see {@link BigdataElement}
//	 */
//	public List<Object> getProperties(final URI uri, final URI prop) {
//
//		try {
//			
//			final RepositoryResult<Statement> result = 
//					getWriteConnection().getStatements(uri, prop, null, false);
//			
//			final List<Object> props = new LinkedList<Object>();
//			
//			while (result.hasNext()) {
//				
//				final Value value = result.next().getObject();
//				
//				if (!(value instanceof Literal)) {
//					throw new RuntimeException("not a property: " + value);
//				}
//				
//				final Literal lit = (Literal) value;
//				
//				props.add(factory.fromLiteral(lit));
//				
//			}
//			
//			return props;
//			
//        } catch (RuntimeException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//		
//	}
	
	/**
	 * Return the property names for an edge or vertex.
     * 
     * @see {@link BigdataElement}
	 */
	public Set<String> getPropertyKeys(final URI uri) {
		
		try {
			
            final RepositoryConnection cxn = readFromWriteConnection ? 
                    getWriteConnection() : getReadConnection();

            final RepositoryResult<Statement> result = 
					cxn.getStatements(uri, null, null, false);
			
			final Set<String> properties = new LinkedHashSet<String>();
			
			while (result.hasNext()) {
				
				final Statement stmt = result.next();
				
				if (!(stmt.getObject() instanceof Literal)) {
					continue;
				}
				
				if (stmt.getPredicate().equals(LABEL)) {
					continue;
				}
				
				final String p = 
						factory.fromURI(stmt.getPredicate());
				
				properties.add(p);
				
			}
			
			return properties;
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}

    /**
     * Remove all values for a particular property on an edge or vertex.
     * 
     * @see {@link BigdataElement}
     */
    public Object removeProperty(final URI uri, final String prop) {
        
        return removeProperty(uri, factory.toPropertyURI(prop));
        
    }
    
	/**
	 * Remove all values for a particular property on an edge or vertex.
     * 
     * @see {@link BigdataElement}
	 */
	public Object removeProperty(final URI uri, final URI prop) {

		try {
			
			final Object oldVal = getProperty(uri, prop);
			
			getWriteConnection().remove(uri, prop, null);
			
			return oldVal;
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	
	}

    /**
     * Set a single-value property on an edge or vertex (remove the old
     * value first).
     * 
     * @see {@link BigdataElement}
     */
    public void setProperty(final URI s, final String prop, final Object val) {

        if (val instanceof Collection) {
            
            @SuppressWarnings("unchecked")
            final Collection<Object> vals = (Collection<Object>) val;
                    
            // empty collection, do nothing
            if (vals.size() == 0) {
                return;
            }
            
            final Collection<Literal> literals = new LinkedList<Literal>();
            
            for (Object o : vals) {
                
                literals.add(factory.toLiteral(o));
                
            }
            
            setProperty(s, factory.toPropertyURI(prop), literals);
            
        } else if (val.getClass().isArray()) {

            final int len = Array.getLength(val);
            
            // empty array, do nothing
            if (len == 0) {
                return;
            }
            
            final Collection<Literal> literals = new LinkedList<Literal>();
            
            for (int i = 0; i < len; i++) {
                
                final Object o = Array.get(val, i);
                
                literals.add(factory.toLiteral(o));
                
            }
            
            setProperty(s, factory.toPropertyURI(prop), literals);
            
        } else {
        
            setProperty(s, factory.toPropertyURI(prop), factory.toLiteral(val));
            
        }

    }
    
	/**
	 * Set a single-value property on an edge or vertex (remove the old
	 * value first).
	 * 
	 * @see {@link BigdataElement}
	 */
	public void setProperty(final URI uri, final URI prop, final Literal val) {
		
		try {

		    final RepositoryConnection cxn = getWriteConnection();
		    
		    if (!laxProperties) {
		        
    		    // remove the old value
    		    cxn.remove(uri, prop, null);
    		    
		    }
		    
		    // add the new value
			cxn.add(uri, prop, val);
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}
	
    /**
     * Set a multi-value property on an edge or vertex (remove the old
     * values first).
     * 
     * @see {@link BigdataElement}
     */
    public void setProperty(final URI uri, final URI prop, 
            final Collection<Literal> vals) {
        
        try {

            final RepositoryConnection cxn = getWriteConnection();
            
            if (!laxProperties) {
                
                // remove the old value
                cxn.remove(uri, prop, null);
                
            }
            
            // add the new values
            for (Literal val : vals) {
                cxn.add(uri, prop, val);
            }
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }
    
//    /**
//     * Add a property on an edge or vertex (multi-value property extension).
//     * 
//     * @see {@link BigdataElement}
//     */
//    public void addProperty(final URI uri, final String prop, final Object val) {
//        
//        setProperty(uri, factory.toPropertyURI(prop), factory.toLiteral(val));
//
//    }
//    
//    /**
//     * Add a property on an edge or vertex (multi-value property extension).
//     * 
//     * @see {@link BigdataElement}
//     */
//    public void addProperty(final URI uri, final URI prop, final Literal val) {
//        
//        try {
//            
//            getWriteConnection().add(uri, prop, val);
//            
//        } catch (RuntimeException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        
//    }
    
    /**
     * Post a GraphML file to the remote server. (Bulk-upload operation.)
     */
    public void loadGraphML(final String file) throws Exception {
        
        GraphMLReader.inputGraph(this, file);
        
    }
    
	/**
	 * Add an edge.
	 */
	@Override
	public Edge addEdge(final Object key, final Vertex from, final Vertex to, 
			final String label) {
		
	    if (log.isInfoEnabled())
	        log.info("("+key+", "+from+", "+to+", "+label+")");
	    
		if (label == null) {
			throw new IllegalArgumentException();
		}
		
		if (key != null && !laxEdges) {
			
			final Edge edge = getEdge(key);
			
			if (edge != null) {
				if (!(edge.getVertex(Direction.OUT).equals(from) &&
						(edge.getVertex(Direction.IN).equals(to)))) {
					throw new IllegalArgumentException("edge already exists: " + key);
				}
			}
			
		}
			
        final String eid = key != null ? key.toString() : UUID.randomUUID().toString();
        
        final URI edgeURI = factory.toEdgeURI(eid);

		try {
				
		    // do we need to check this?
//			if (cxn().hasStatement(edgeURI, TYPE, EDGE, false)) {
//				throw new IllegalArgumentException("edge " + eid + " already exists");
//			}

			final URI fromURI = factory.toVertexURI(from.getId().toString());
			final URI toURI = factory.toVertexURI(to.getId().toString());
			
			final RepositoryConnection cxn = getWriteConnection();
			cxn.add(fromURI, edgeURI, toURI);
			cxn.add(edgeURI, TYPE, EDGE);
			cxn.add(edgeURI, LABEL, factory.toLiteral(label));
			
			return new BigdataEdge(new StatementImpl(fromURI, edgeURI, toURI), this);
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}

	/**
	 * Add a vertex.
	 */
	@Override
	public Vertex addVertex(final Object key) {
		
	    if (log.isInfoEnabled())
	        log.info("("+key+")");
	    
		try {
			
			final String vid = key != null ? 
					key.toString() : UUID.randomUUID().toString();
					
			final URI uri = factory.toVertexURI(vid);

            // do we need to check this?
//			if (cxn().hasStatement(vertexURI, TYPE, VERTEX, false)) {
//				throw new IllegalArgumentException("vertex " + vid + " already exists");
//			}
			
			getWriteConnection().add(uri, TYPE, VERTEX);

			return new BigdataVertex(uri, this);
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}

	/**
	 * Lookup an edge.
	 */
	@Override
	public Edge getEdge(final Object key) {
	    
	    if (log.isInfoEnabled())
	        log.info("("+key+")");
		
		if (key == null)
			throw new IllegalArgumentException();
		
		try {
			
			final URI edge = factory.toEdgeURI(key.toString());
			
            final RepositoryConnection cxn = readFromWriteConnection ? 
                    getWriteConnection() : getReadConnection();
                    
			final RepositoryResult<Statement> result = 
					cxn.getStatements(null, edge, null, false);
			
			if (result.hasNext()) {
				
				final Statement stmt = result.next();
				
				if (result.hasNext()) {
					throw new RuntimeException(
							"duplicate edge: " + key);
				}
				
				return new BigdataEdge(stmt, this);
				
			}
			
			return null;
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}

	/**
	 * Iterate all edges.
	 */
	@Override
	public Iterable<Edge> getEdges() {
		
        if (log.isInfoEnabled())
            log.info("()");
        
		final URI wild = null;
		return getEdges(wild, wild);
		
	}
	
	/**
     * Find edges based on the from and to vertices and the edge labels, all
     * optional parameters (can be null). The edge labels can be null to include
     * all labels.
     * <p>
     * 
     * @param from
     *            the from vertex (null for wildcard)
     * @param to
     *            the to vertex (null for wildcard)
     * @param labels
     *            the edge labels to consider (optional)
     * @return the edges matching the supplied criteria
     */
	Iterable<Edge> getEdges(final URI from, final URI to, final String... labels) {

	    final GraphQueryResult stmts = getElements(from, to, labels);
	    
        return new EdgeIterable(stmts);

	}

	/**
	 * Translates the request to a high-performance SPARQL query:
     * 
     * construct {
     *   ?from ?edge ?to .
     * } where {
     *   ?edge rdf:type <Edge> .
     *   
     *   ?from ?edge ?to .
     *   
     *   # filter by edge label
     *   ?edge rdfs:label ?label .
     *   filter(?label in ("label1", "label2", ...)) .
     * }
	 */
	protected GraphQueryResult getElements(final URI from, final URI to, 
	        final String... labels) {
	    
        final StringBuilder sb = new StringBuilder();
        sb.append("construct { ?from ?edge ?to . } where {\n");
        sb.append("  ?edge <"+TYPE+"> <"+EDGE+"> .\n");
        sb.append("  ?from ?edge ?to .\n");
        if (labels != null && labels.length > 0) {
            if (labels.length == 1) {
                sb.append("  ?edge <"+LABEL+"> \"").append(labels[0]).append("\" .\n");
            } else {
                sb.append("  ?edge <"+LABEL+"> ?label .\n");
                sb.append("  filter(?label in (");
                for (String label : labels) {
                    sb.append("\""+label+"\", ");
                }
                sb.setLength(sb.length()-2);
                sb.append(")) .\n");
            }
        }
        sb.append("}");

        // bind the from and/or to
        final String queryStr = sb.toString()
                    .replace("?from", from != null ? "<"+from+">" : "?from")
                        .replace("?to", to != null ? "<"+to+">" : "?to");
     
        try {
            
            final RepositoryConnection cxn = readFromWriteConnection ? 
                    getWriteConnection() : getReadConnection();
                    
            final org.openrdf.query.GraphQuery query = 
                    cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
            
            final GraphQueryResult stmts = query.evaluate();

            return stmts;
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
	}
	
	/**
	 * Find edges based on a SPARQL construct query.  The query MUST construct
	 * edge statements: 
	 * <p>
	 * construct { ?from ?edge ?to } where { ... }
	 * 
	 * @see {@link BigdataGraphQuery}
	 */
	Iterable<Edge> getEdges(final String queryStr) { 
	    
	    try {
	        
            final RepositoryConnection cxn = readFromWriteConnection ? 
                    getWriteConnection() : getReadConnection();
                    
			final org.openrdf.query.GraphQuery query = 
					cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
			
			final GraphQueryResult stmts = query.evaluate();
			
			return new EdgeIterable(stmts);

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}

    /**
     * Find vertices based on the supplied from and to vertices and the edge 
     * labels.  One or the other (from and to) must be null (wildcard), but not 
     * both. Use getEdges() for wildcards on both the from and to.  The edge 
     * labels can be null to include all labels.
     * 
     * @param from
     *             the from vertex (null for wildcard)
     * @param to
     *             the to vertex (null for wildcard)
     * @param labels
     *             the edge labels to consider (optional)
     * @return
     *             the vertices matching the supplied criteria
     */
	Iterable<Vertex> getVertices(final URI from, final URI to, 
			final String... labels) {
		
		if (from != null && to != null) {
			throw new IllegalArgumentException();
		}
		
		if (from == null && to == null) {
			throw new IllegalArgumentException();
		}
		
        final GraphQueryResult stmts = getElements(from, to, labels);
        
        return new VertexIterable(stmts, from == null);
		
	}
	
    /**
     * Find vertices based on a SPARQL construct query. If the subject parameter
     * is true, the vertices will be taken from the subject position of the
     * constructed statements, otherwise they will be taken from the object
     * position.
     * 
     * @see {@link BigdataGraphQuery}
     */
	Iterable<Vertex> getVertices(final String queryStr, final boolean subject) {
	    
	    try {
	        
            final RepositoryConnection cxn = readFromWriteConnection ? 
                    getWriteConnection() : getReadConnection();
                    
			final org.openrdf.query.GraphQuery query = 
					cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
			
			final GraphQueryResult stmts = query.evaluate();
			
			return new VertexIterable(stmts, subject);
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}
	
	/**
	 * Find edges with the supplied property value.
	 * 
	 * construct {
     *   ?from ?edge ?to .
     * }
     * where {
     *   ?edge <prop> <val> .
     *   ?from ?edge ?to .
     * }
	 */
	@Override
	public Iterable<Edge> getEdges(final String prop, final Object val) {
		
        if (log.isInfoEnabled())
            log.info("("+prop+", "+val+")");
        
		final URI p = factory.toPropertyURI(prop);
		final Literal o = factory.toLiteral(val);
		
		try {
		
	        final StringBuilder sb = new StringBuilder();
	        sb.append("construct { ?from ?edge ?to . } where {\n");
	        sb.append("  ?edge <"+p+"> "+o+" .\n");
	        sb.append("  ?from ?edge ?to .\n");
	        sb.append("}");

	        final String queryStr = sb.toString();

			return getEdges(queryStr);
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}

	/**
	 * Lookup a vertex.
	 */
	@Override
	public Vertex getVertex(final Object key) {
		
        if (log.isInfoEnabled())
            log.info("("+key+")");
        
		if (key == null)
			throw new IllegalArgumentException();
		
		final URI uri = factory.toVertexURI(key.toString());
		
		try {
		    
            final RepositoryConnection cxn = readFromWriteConnection ? 
                    getWriteConnection() : getReadConnection();
                    
			if (cxn.hasStatement(uri, TYPE, VERTEX, false)) {
				return new BigdataVertex(uri, this);
			}
			
			return null;
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}

	
    /**
     * Iterate all vertices.
     */
	@Override
	public Iterable<Vertex> getVertices() {
		
        if (log.isInfoEnabled())
            log.info("()");
        
		try {
		    
            final RepositoryConnection cxn = readFromWriteConnection ? 
                    getWriteConnection() : getReadConnection();
                    
			final RepositoryResult<Statement> result = 
					cxn.getStatements(null, TYPE, VERTEX, false);
			
			return new VertexIterable(result, true);
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}

    /**
     * Find vertices with the supplied property value.
     */
	@Override
	public Iterable<Vertex> getVertices(final String prop, final Object val) {
		
        if (log.isInfoEnabled())
            log.info("("+prop+", "+val+")");
        
		final URI p = factory.toPropertyURI(prop);
		final Literal o = factory.toLiteral(val);
		
		try {
		    
            final RepositoryConnection cxn = readFromWriteConnection ? 
                    getWriteConnection() : getReadConnection();
                    
			final RepositoryResult<Statement> result = 
					cxn.getStatements(null, p, o, false);
			
			return new VertexIterable(result, true);
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}

	/**
	 * Providing an override implementation for our GraphQuery to avoid the
	 * low-performance scan and filter paradigm. See {@link BigdataGraphQuery}. 
	 */
	@Override
	public GraphQuery query() {

	    if (log.isInfoEnabled())
            log.info("()");
        
//		return new DefaultGraphQuery(this);
	    return new BigdataGraphQuery(this);
	}

	/**
	 * Remove an edge and its properties.
	 */
	@Override
	public void removeEdge(final Edge edge) {
	    
		try {
		    
			final URI uri = factory.toURI(edge);
			
            if (!getWriteConnection().hasStatement(uri, TYPE, EDGE, false)) {
                throw new IllegalStateException();
            }
            
            final URI wild = null;
            
			// remove the edge statement
			getWriteConnection().remove(wild, uri, wild);
			
			// remove its properties
			getWriteConnection().remove(uri, wild, wild);
			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		
	}

	/**
	 * Remove a vertex and its edges and properties.
	 */
	@Override
	public void removeVertex(final Vertex vertex) {
	    
		try {
		    
			final URI uri = factory.toURI(vertex);
			
            if (!getWriteConnection().hasStatement(uri, TYPE, VERTEX, false)) {
                throw new IllegalStateException();
            }
            
            final URI wild = null;
            
			// remove outgoing edges and properties
			getWriteConnection().remove(uri, wild, wild);
			
			// remove incoming edges
			getWriteConnection().remove(wild, wild, uri);
			
		} catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}

	/**
	 * Translate a collection of Bigdata statements into an iteration of
	 * Blueprints vertices.
	 *  
	 * @author mikepersonick
	 *
	 */
	public class VertexIterable implements Iterable<Vertex>, Iterator<Vertex> {

		private final CloseableIteration<Statement, ? extends OpenRDFException> stmts;
		
		private final boolean subject;
		
		private final List<Vertex> cache;
		
		public VertexIterable(
				final CloseableIteration<Statement, ? extends OpenRDFException> stmts,
				final boolean subject) {
			this.stmts = stmts;
			this.subject = subject;
			this.cache = new LinkedList<Vertex>();
		}
		
		@Override
		public boolean hasNext() {
			try {
				return stmts.hasNext();
			} catch (OpenRDFException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Vertex next() {
			try {
				final Statement stmt = stmts.next();
				final URI v = (URI) 
						(subject ? stmt.getSubject() : stmt.getObject());
				if (!hasNext()) {
					stmts.close();
				}
				final Vertex vertex = new BigdataVertex(v, BigdataGraph.this);
				cache.add(vertex);
				return vertex;
			} catch (OpenRDFException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<Vertex> iterator() {
			return hasNext() ? this : cache.iterator();
		}
		
	}

    /**
     * Translate a collection of Bigdata statements into an iteration of
     * Blueprints edges.
     *  
     * @author mikepersonick
     *
     */
	public class EdgeIterable implements Iterable<Edge>, Iterator<Edge> {

		private final CloseableIteration<Statement, ? extends OpenRDFException> stmts;
		
		private final List<Edge> cache;
		
		public EdgeIterable(
				final CloseableIteration<Statement, ? extends OpenRDFException> stmts) {
			this.stmts = stmts;
			this.cache = new LinkedList<Edge>();
		}
		
		@Override
		public boolean hasNext() {
			try {
				return stmts.hasNext();
			} catch (OpenRDFException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Edge next() {
			try {
				final Statement stmt = stmts.next();
				if (!hasNext()) {
					stmts.close();
				}
				final Edge edge = new BigdataEdge(stmt, BigdataGraph.this);
				cache.add(edge);
				return edge;
			} catch (OpenRDFException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<Edge> iterator() {
			return hasNext() ? this : cache.iterator();
		}
		
	}

    /**
     * Fuse two iterables together into one.  Useful for combining IN and OUT
     * edges for a vertex.
     */
    public final <T> Iterable<T> fuse(final Iterable<T>... args) {
        
        return new FusedIterable<T>(args);
    }
    
    /**
     * Fuse two iterables together into one.  Useful for combining IN and OUT
     * edges for a vertex.
     *  
     * @author mikepersonick
     */
	public class FusedIterable<T> implements Iterable<T>, Iterator<T> {
		
		private final Iterable<T>[] args;
		
		private transient int i = 0;
		
		private transient Iterator<T> curr;
		
		public FusedIterable(final Iterable<T>... args) {
			this.args = args;
			this.curr = args[0].iterator();
		}
		
		@Override
		public boolean hasNext() {
			if (curr.hasNext()) {
				return true;
			}
			while (!curr.hasNext() && i < (args.length-1)) {
				curr = args[++i].iterator();
				if (curr.hasNext()) {
					return true;
				}
			}
			return false;
		}

		@Override
		public T next() {
			return curr.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public Iterator<T> iterator() {
			return this;
		}

	}
	
	/**
	 * Project a subgraph using a SPARQL query.
	 */
	public BigdataGraphlet project(final String queryStr) throws Exception {
	    
        try {
            
            final RepositoryConnection cxn = readFromWriteConnection ? 
                    getWriteConnection() : getReadConnection();
            
            try {
                
                final org.openrdf.query.GraphQuery query = 
                        cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
                
                final GraphQueryResult result = query.evaluate();
                try {
                    
                    final BigdataQueryProjection projection = 
                            new BigdataQueryProjection(factory);
                    
                    return projection.convert(result);
                
                } finally {
                    result.close();
                }
                
            } finally {
            
                cxn.close();
                
            }
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

	}
	
    /**
     * Select results using a SPARQL query.
     */
    public BigdataSelection select(final String queryStr) throws Exception {
        
        try {
            
            final RepositoryConnection cxn = readFromWriteConnection ? 
                    getWriteConnection() : getReadConnection();
            
            try {
                
                final TupleQuery query = (TupleQuery) 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
                
                final TupleQueryResult result = query.evaluate();
                try {
                    
                    final BigdataQueryProjection projection = 
                            new BigdataQueryProjection(factory);
                    
                    return projection.convert(result);
                    
                } finally {
                    result.close();
                }
                
            } finally {
            
                cxn.close();
                
            }
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    
    /**
     * Update graph using SPARQL Update.
     */
    public void update(final String queryStr) throws Exception {
        
        try {
            
            final RepositoryConnection cxn = getWriteConnection();
            
            final Update update = 
                    cxn.prepareUpdate(QueryLanguage.SPARQL, queryStr);
            
            update.execute();
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    
    protected static final Features FEATURES = new Features();

    @Override
    public Features getFeatures() {

        return FEATURES;
        
    }
    
    static {
        
        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = true;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = true;
        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.isWrapper = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = false;
        FEATURES.supportsEdgeIndex = false;
        FEATURES.ignoresSuppliedIds = false;
        FEATURES.supportsTransactions = false;
        FEATURES.supportsIndices = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = false;
        
    }
    
}
