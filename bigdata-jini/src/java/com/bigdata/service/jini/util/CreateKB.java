/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jan 10, 2009
 */

package com.bigdata.service.jini.util;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.master.TaskMaster;
import com.bigdata.util.NV;

/**
 * Utility to create a scale-out KB instance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CreateKB {
    
    private static final Logger log = Logger.getLogger(CreateKB.class);

	/**
	 * The name of the component in the jini configuration file for this class.
	 */
    protected static final String COMPONENT = CreateKB.class.getName();

	/**
	 * Configuration options understood by this utility. These options must be
	 * specified for the {@value CreateKB#COMPONENT} component in the jini
	 * configuration file.
	 */
    public interface ConfigurationOptions {

		/**
		 * The KB namespace.
		 */
		String NAMESPACE = "namespace";

		/** An {@link NV}[] providing the configuration properties for the KB. */
		String PROPERTIES = "properties";

    }
    
    private final JiniFederation<?> fed;
    
    private CreateKB(final JiniFederation<?> fed) {
    	
    	if(fed == null)
    		throw new IllegalArgumentException();
    	
    	this.fed = fed;
    	
    }

	/**
	 * Create the {@link AbstractTripleStore} specified by
	 * {@link ConfigurationOptions#NAMESPACE} using the <code>properties</code>
	 * associated with the {@link TaskMaster.JobState#component}.
	 * 
	 * @return <code>true</code> if a new KB instance was created.
	 *         <code>false</code> if the named KB instance already exists.
	 */
    protected boolean createTripleStore() throws ConfigurationException {
        
        /*
         * Pick up properties configured for the client as defaults.
         * 
         * You must specify those properties using NV[] for this component.
         */
        final Properties properties = fed.getClient().getProperties(
                COMPONENT);

		final String namespace = (String) fed
				.getClient()
				.getConfiguration()
				.getEntry(COMPONENT, ConfigurationOptions.NAMESPACE,
						String.class);

		System.out.println("KB namespace=" + namespace);

		// Locate the resource declaration (aka "open"). This tells us if it
		// exists already.
		AbstractTripleStore tripleStore = (AbstractTripleStore) fed
				.getResourceLocator().locate(namespace, ITx.UNISOLATED);

		if (tripleStore != null) {

			System.out.println("exists: " + namespace);

			// Triple store already exists with that namespace.
			return false; // pre-existing.
			
		}

		/*
		 * Create the KB instance.
		 */

		if (log.isInfoEnabled()) {
			log.info("Creating KB instance: namespace="+namespace);
			log.info("Properties=" + properties.toString());
		}

		tripleStore = new ScaleOutTripleStore(fed, namespace, ITx.UNISOLATED,
				properties);

        // create the triple store.
        tripleStore.create();

        System.out.println("Created tripleStore: " + namespace);

        // show #of statements in the newly create triple store (e.g., any axioms).
        System.out.println("axiomCount=" + tripleStore.getStatementCount());
        
        // New KB instance was created.
        return true;

    }

	/**
	 * Creates a KB instance.
	 * <p>
	 * Configuration options use {@link #COMPONENT} as their namespace. The
	 * following options are defined:
	 * <dl>
	 * 
	 * <dt>{@value ConfigurationOptions#NAMESPACE}</dt>
	 * <dd>The namespace of the KB instance.</dd>
	 * 
	 * <dt>{@value ConfigurationOptions#PROPERTIES}</dt>
	 * <dd>The properties used to create the KB instance expressed as an
	 * {@link NV}[].</dd>
	 * 
	 * </dl>
	 * 
	 * @param args
	 *            Configuration file and optional overrides.
	 * 
	 * @see ConfigurationOptions
	 * 
	 * @throws ConfigurationException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
    public static void main(final String[] args) throws InterruptedException,
            ConfigurationException, IOException, ExecutionException {

        final JiniFederation<?> fed = JiniClient.newInstance(args).connect();

        /*
         * Install a shutdown hook (normal kill will trigger this hook).
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {

                fed.shutdownNow();

            }

        });
        
        final boolean created;
        try {

			created = new CreateKB(fed).createTripleStore();
        	
        } finally {
        
        	fed.shutdown();

        }
        
		System.exit(created ? 0 : 1);

    }

}
