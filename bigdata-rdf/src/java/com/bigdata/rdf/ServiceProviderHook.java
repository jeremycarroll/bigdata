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
 * Created on Feb 27, 2012
 */

package com.bigdata.rdf;

import info.aduna.lang.service.ServiceRegistry;

import java.util.ServiceLoader;

import org.openrdf.query.QueryLanguage;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriterRegistry;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.nquads.NQuadsParser;
import com.bigdata.rdf.rio.ntriples.BigdataNTriplesParserFactory;
import com.bigdata.rdf.rio.rdfxml.BigdataRDFXMLParserFactory;
import com.bigdata.rdf.rio.rdfxml.BigdataRDFXMLWriterFactory;

/**
 * This static class provides a hook which allows the replacement of services
 * registered via the openrdf {@link ServiceRegistry} pattern which makes use of
 * the same underlying pattern which is disclosed by the {@link ServiceLoader}.
 * <p>
 * The {@link ServiceRegistry} pattern provides a declarative association
 * between a service and a service provider. The service declarations are
 * located in <code>META-INF/services/</code> packages. Each file in such a
 * package name the service interface and the contents of the file name the
 * service provider(s). The set of all such service provides located in all such
 * service packages is registered for a given service interface. For openrdf,
 * the service provides are initially associated with a <i>key</i>, such as an
 * {@link RDFFormat}, {@link QueryLanguage}, etc.
 * <p>
 * However, this service registration pattern does not support the specification
 * of a <em>preferred</em> service provider. In the context of multiple service
 * providers for the same <i>key</i> and service interface, there is no way to
 * control which service provider will remain in the {@link ServiceRegistry}.
 * <p>
 * This effects things such as the bigdata extension for the RDF/XML parser
 * which adds support for SIDs mode interchange and the interchange of
 * {@link StatementEnum} metadata.
 * <p>
 * This class is used to "hook" the various service registeries and force the
 * use of the bigdata extension when it adds semantics not present in the base
 * service provider implementation. For such "hooked" services providers, the
 * service registry pattern using <code>META-INF/services</code> is not
 * manditory, but following the pattern is never the less recommended.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/439">Class
 *      loader problems </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServiceProviderHook {

    static private boolean loaded = false;
    static {
        forceLoad();
    }
    
    /**
     * This hook may be used to force the load of this class so it can ensure
     * that the bigdata version of a service provider is used instead of the
     * openrdf version. This is NOT optional. Without this hook, we do not have
     * control over which version is resolved last in the processed
     * <code>META-INF/services</code> files.
     */
    synchronized static public void forceLoad() {
        
        if (loaded)
            return;

        /*
         * Force the class loader to resolve the register, which will cause it
         * to be populated with the service provides as declared in the various
         * META-INF/services/serviceIface files.
         * 
         * Once that step is known to be complete, we override the service
         * provider for RDF/XML.
         */
        {

            final RDFParserRegistry r = RDFParserRegistry.getInstance();

            r.add(new BigdataRDFXMLParserFactory());

            // Note: This ensures that the RDFFormat for NQuads is loaded.
            r.get(NQuadsParser.nquads);

            r.add(new BigdataNTriplesParserFactory());
            
        }

        // Ditto, but for the writer.
        {
            final RDFWriterRegistry r = RDFWriterRegistry.getInstance();

            r.add(new BigdataRDFXMLWriterFactory());

        }

//        {
//            final PropertiesParserRegistry r = PropertiesParserRegistry.getInstance();
//            
//            r.add(new PropertiesXMLParserFactory());
//            
//            r.add(new PropertiesTextParserFactory());
//            
//        }
//        
//        {
//            final PropertiesWriterRegistry r = PropertiesWriterRegistry.getInstance();
//            
//            r.add(new PropertiesXMLWriterFactory());
//            
//            r.add(new PropertiesTextWriterFactory());
//            
//        }
        
        loaded = true;
        
    }

}
