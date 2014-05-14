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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;

import com.bigdata.rdf.store.BD;
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

    /**
     * URI used to represent a Vertex.
     */
	public static final URI VERTEX = new URIImpl(BD.NAMESPACE + "Vertex");
	
    /**
     * URI used to represent a Edge.
     */
	public static final URI EDGE = new URIImpl(BD.NAMESPACE + "Edge");
	
	/**
	 * Factory for round-tripping between Blueprints data and RDF data.
	 */
	final BlueprintsRDFFactory factory;
	
	public BigdataGraph(final BlueprintsRDFFactory factory) {

	    this.factory = factory;
	    
	}
	
	/**
	 * For some reason this is part of the specification (i.e. part of the
	 * Blueprints test suite).
	 */
	public String toString() {
	    
	    return getClass().getSimpleName().toLowerCase();
	    
	}
	
    /**
     * Different implementations will return different types of connections
     * depending on the mode (client/server, embedded, read-only, etc.)
     */
	protected abstract RepositoryConnection cxn() throws Exception;
	
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
			
			final RepositoryResult<Statement> result = 
					cxn().getStatements(uri, prop, null, false);
			
			if (result.hasNext()) {
				
				final Value value = result.next().getObject();
				
				if (result.hasNext()) {
					throw new RuntimeException(uri
							+ ": more than one value for p: " + prop
							+ ", did you mean to call getProperties()?");
				}

				if (!(value instanceof Literal)) {
					throw new RuntimeException("not a property: " + value);
				}
			
				final Literal lit = (Literal) value;
				
				return factory.fromLiteral(lit);
			
			}
			
			return null;
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}
	
    /**
     * Return a multi-valued property for an edge or vertex.
     * 
     * @see {@link BigdataElement}
     */
    public List<Object> getProperties(final URI uri, final String prop) {
        
        return getProperties(uri, factory.toPropertyURI(prop));
        
    }


	/**
     * Return a multi-valued property for an edge or vertex.
     * 
     * @see {@link BigdataElement}
	 */
	public List<Object> getProperties(final URI uri, final URI prop) {

		try {
			
			final RepositoryResult<Statement> result = 
					cxn().getStatements(uri, prop, null, false);
			
			final List<Object> props = new LinkedList<Object>();
			
			while (result.hasNext()) {
				
				final Value value = result.next().getObject();
				
				if (!(value instanceof Literal)) {
					throw new RuntimeException("not a property: " + value);
				}
				
				final Literal lit = (Literal) value;
				
				props.add(factory.fromLiteral(lit));
				
			}
			
			return props;
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}
	
	/**
	 * Return the property names for an edge or vertex.
     * 
     * @see {@link BigdataElement}
	 */
	public Set<String> getPropertyKeys(final URI uri) {
		
		try {
			
			final RepositoryResult<Statement> result = 
					cxn().getStatements(uri, null, null, false);
			
			final Set<String> properties = new LinkedHashSet<String>();
			
			while (result.hasNext()) {
				
				final Statement stmt = result.next();
				
				if (!(stmt.getObject() instanceof Literal)) {
					continue;
				}
				
				if (stmt.getPredicate().equals(RDFS.LABEL)) {
					continue;
				}
				
				final String p = 
						factory.fromPropertyURI(stmt.getPredicate());
				
				properties.add(p);
				
			}
			
			return properties;
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
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
			
			cxn().remove(uri, prop, null);
			
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
    public void setProperty(final URI uri, final String prop, final Object val) {
        
        setProperty(uri, factory.toPropertyURI(prop), factory.toLiteral(val));

    }
    
	/**
	 * Set a single-value property on an edge or vertex (remove the old
	 * value first).
	 * 
	 * @see {@link BigdataElement}
	 */
	public void setProperty(final URI uri, final URI prop, final Literal val) {
		
		try {
			
			cxn().remove(uri, prop, null);
			
			cxn().add(uri, prop, val);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
    /**
     * Add a property on an edge or vertex (multi-value property extension).
     * 
     * @see {@link BigdataElement}
     */
    public void addProperty(final URI uri, final String prop, final Object val) {
        
        setProperty(uri, factory.toPropertyURI(prop), factory.toLiteral(val));

    }
    
    /**
     * Add a property on an edge or vertex (multi-value property extension).
     * 
     * @see {@link BigdataElement}
     */
    public void addProperty(final URI uri, final URI prop, final Literal val) {
        
        try {
            
            cxn().add(uri, prop, val);
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }
    
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
		
		if (label == null) {
			throw new IllegalArgumentException();
		}
		
		final String eid = key != null ? key.toString() : UUID.randomUUID().toString();
		
		final URI edgeURI = factory.toEdgeURI(eid);

		if (key != null) {
			
			final Edge edge = getEdge(key);
			
			if (edge != null) {
				if (!(edge.getVertex(Direction.OUT).equals(from) &&
						(edge.getVertex(Direction.OUT).equals(to)))) {
					throw new IllegalArgumentException("edge already exists: " + key);
				}
			}
			
		}
			
		try {
				
		    // do we need to check this?
//			if (cxn().hasStatement(edgeURI, RDF.TYPE, EDGE, false)) {
//				throw new IllegalArgumentException("edge " + eid + " already exists");
//			}

			final URI fromURI = factory.toVertexURI(from.getId().toString());
			final URI toURI = factory.toVertexURI(to.getId().toString());
			
			cxn().add(fromURI, edgeURI, toURI);
			cxn().add(edgeURI, RDF.TYPE, EDGE);
			cxn().add(edgeURI, RDFS.LABEL, factory.toLiteral(label));
			
			return new BigdataEdge(new StatementImpl(fromURI, edgeURI, toURI), this);
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	/**
	 * Add a vertex.
	 */
	@Override
	public Vertex addVertex(final Object key) {
		
		try {
			
			final String vid = key != null ? 
					key.toString() : UUID.randomUUID().toString();
					
			final URI uri = factory.toVertexURI(vid);

            // do we need to check this?
//			if (cxn().hasStatement(vertexURI, RDF.TYPE, VERTEX, false)) {
//				throw new IllegalArgumentException("vertex " + vid + " already exists");
//			}
			
			cxn().add(uri, RDF.TYPE, VERTEX);

			return new BigdataVertex(uri, this);
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	/**
	 * Lookup an edge.
	 */
	@Override
	public Edge getEdge(final Object key) {
		
		if (key == null)
			throw new IllegalArgumentException();
		
		try {
			
			final URI edge = factory.toEdgeURI(key.toString());
			
			final RepositoryResult<Statement> result = 
					cxn().getStatements(null, edge, null, false);
			
			if (result.hasNext()) {
				
				final Statement stmt = result.next();
				
				if (result.hasNext()) {
					throw new RuntimeException(
							"duplicate edge: " + key);
				}
				
				return new BigdataEdge(stmt, this);
				
			}
			
			return null;
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	/**
	 * Iterate all edges.
	 */
	@Override
	public Iterable<Edge> getEdges() {
		
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
        sb.append("  ?edge rdf:type bd:Edge .\n");
        sb.append("  ?from ?edge ?to .\n");
        if (labels != null && labels.length > 0) {
            if (labels.length == 1) {
                sb.append("  ?edge rdfs:label \"").append(labels[0]).append("\" .\n");
            } else {
                sb.append("  ?edge rdfs:label ?label .\n");
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
            
            final org.openrdf.query.GraphQuery query = 
                    cxn().prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
            
            final GraphQueryResult stmts = query.evaluate();

            return stmts;
            
        } catch (Exception ex) {
            throw new RuntimeException(ex);
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
	        
			final org.openrdf.query.GraphQuery query = 
					cxn().prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
			
			final GraphQueryResult stmts = query.evaluate();
			
			return new EdgeIterable(stmts);

		} catch (Exception ex) {
			throw new RuntimeException(ex);
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
	        
			final org.openrdf.query.GraphQuery query = 
					cxn().prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
			
			final GraphQueryResult stmts = query.evaluate();
			
			return new VertexIterable(stmts, subject);
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
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
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	/**
	 * Lookup a vertex.
	 */
	@Override
	public Vertex getVertex(final Object key) {
		
		if (key == null)
			throw new IllegalArgumentException();
		
		final URI uri = factory.toVertexURI(key.toString());
		
		try {
		    
			if (cxn().hasStatement(uri, RDF.TYPE, VERTEX, false)) {
				return new BigdataVertex(uri, this);
			}
			
			return null;
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	
    /**
     * Iterate all vertices.
     */
	@Override
	public Iterable<Vertex> getVertices() {
		
		try {
		    
			final RepositoryResult<Statement> result = 
					cxn().getStatements(null, RDF.TYPE, VERTEX, false);
			
			return new VertexIterable(result, true);
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

    /**
     * Find vertices with the supplied property value.
     */
	@Override
	public Iterable<Vertex> getVertices(final String prop, final Object val) {
		
		final URI p = factory.toPropertyURI(prop);
		final Literal o = factory.toLiteral(val);
		
		try {
		    
			final RepositoryResult<Statement> result = 
					cxn().getStatements(null, p, o, false);
			
			return new VertexIterable(result, true);
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	/**
	 * Providing an override implementation for our GraphQuery to avoid the
	 * low-performance scan and filter paradigm. See {@link BigdataGraphQuery}. 
	 */
	@Override
	public GraphQuery query() {
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
			
            if (!cxn().hasStatement(uri, RDF.TYPE, EDGE, false)) {
                throw new IllegalStateException();
            }
            
            final URI wild = null;
            
			// remove the edge statement
			cxn().remove(wild, uri, wild);
			
			// remove its properties
			cxn().remove(uri, wild, wild);
			
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
			
            if (!cxn().hasStatement(uri, RDF.TYPE, VERTEX, false)) {
                throw new IllegalStateException();
            }
            
            final URI wild = null;
            
			// remove outgoing edges and properties
			cxn().remove(uri, wild, wild);
			
			// remove incoming edges
			cxn().remove(wild, wild, uri);
			
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
        FEATURES.supportsPrimitiveArrayProperty = false;
        FEATURES.supportsUniformListProperty = false;
        FEATURES.supportsMixedListProperty = false;
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
        FEATURES.ignoresSuppliedIds = true;
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
