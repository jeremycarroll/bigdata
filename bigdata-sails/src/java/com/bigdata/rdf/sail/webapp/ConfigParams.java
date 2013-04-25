/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Interface declaring the <code>config-param</code>s understood by the
 * {@link BigdataRDFServletContextListener}.
 * <p>
 * Note: When used in a jini/River configuration, the name of the component for
 * those configuration options is the fully qualified class name for the
 * {@link NanoSparqlServer}.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/596"> Change
 *      web.xml parameter names to be consistent with Jini/River </a>
 */
public interface ConfigParams {

    /**
     * The property file (for a standalone bigdata instance) or the jini
     * configuration file (for a bigdata federation). The file must end with
     * either ".properties" or ".config".
     */
    String PROPERTY_FILE = "propertyFile";

    /**
     * The default bigdata namespace of for the triple or quad store instance to
     * be exposed (default {@link #DEFAULT_NAMESPACE}). Note that there can be
     * many triple or quad store instances within a bigdata instance.
     */
    String NAMESPACE = "namespace";
    
    String DEFAULT_NAMESPACE = "kb";

    /**
     * When <code>true</code>, an instance of the specified {@link #NAMESPACE}
     * will be created if none exists.
     */
    String CREATE = "create";
    
    boolean DEFAULT_CREATE = true;
    
    /**
     * The size of the thread pool used to service SPARQL queries -OR- ZERO
     * (0) for an unbounded thread pool (default
     * {@value #DEFAULT_QUERY_THREAD_POOL_SIZE}).
     */
    String QUERY_THREAD_POOL_SIZE = "queryThreadPoolSize";
    
    int DEFAULT_QUERY_THREAD_POOL_SIZE = 16;
    
    /**
     * Force a compacting merge of all shards on all data services in a
     * bigdata federation (optional, default <code>false</code>).
     * 
     * <strong>This option should only be used for benchmarking
     * purposes.</strong>
     */
    String FORCE_OVERFLOW = "forceOverflow";

    /**
     * The commit time against which the server will assert a read lock by
     * holding open a read-only transaction against that commit point
     * (optional). When given, queries will default to read against this
     * commit point. Otherwise queries will default to read against the most
     * recent commit point on the database. Regardless, each query will be
     * issued against a read-only transaction.
     */
    String READ_LOCK = "readLock";
    
    /**
     * When <code>true</code> and the KB instance is in the <code>quads</code>
     * mode, each named graph will also be described in in the same level of
     * detail as the default graph (default
     * {@value #DEFAULT_DESCRIBE_EACH_NAMED_GRAPH}). Otherwise only the default
     * graph will be described.
     * <p>
     * Note: I have changed the default to <code>false</code> since this
     * operation can be long-running for messy web graphs such as the TBL six
     * degrees of freedom crawl. We wind up with one named graph for each
     * "friend", which is a lot of named graphs, plus there are a lot of
     * predicates that show up in web data. Together, this makes the per-named
     * graph response not a very good default. However, you can certainly enable
     * this if you only have a reasonable number of named graphs and/or only
     * expose the SPARQL end point to a limited audience.
     */
    String DESCRIBE_EACH_NAMED_GRAPH = "describeEachNamedGraph";
    
    boolean DEFAULT_DESCRIBE_EACH_NAMED_GRAPH = false;
    
    /**
     * When <code>true</code>, requests will be refused for mutation operations
     * on the database made through the REST API. This may be used to help lock
     * down a public facing interface.
     */
    String READ_ONLY = "readOnly";

    boolean DEFAULT_READ_ONLY = false;

    /**
     * When non-zero, this specifies the timeout (milliseconds) for a query.
     * This may be used to limit resource consumption on a public facing
     * interface.
     */
    String QUERY_TIMEOUT = "queryTimeout";

    long DEFAULT_QUERY_TIMEOUT = 0L;

    /**
     * A class that extends {@link BigdataRDFServletContextListener}. This
     * offers applications a means to hook the {@link ServletContextListener}
     * methods.
     * <p>
     * Note:
     * 
     * @see BigdataRDFServletContextListener#contextInitialized(ServletContextEvent)
     * @see BigdataRDFServletContextListener#contextDestroyed(ServletContextEvent)
     * @see #DEFAULT_SERVLET_CONTEXT_LISTENER_CLASS
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/667" >
     *      Provide NanoSparqlServer initialization hook </a>
     */
    String SERVLET_CONTEXT_LISTENER_CLASS = "servletContextListenerClass";

    String DEFAULT_SERVLET_CONTEXT_LISTENER_CLASS = BigdataRDFServletContextListener.class
            .getName();

}
