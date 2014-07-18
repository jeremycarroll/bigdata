/**
Copyright (C) SYSTAP, LLC 2006-Infinity.  All rights reserved.

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

import org.apache.commons.configuration.Configuration;

import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationContext;
import com.tinkerpop.rexster.config.GraphConfigurationException;

/**
 * Create and configure a BigdataGraph for Rexster.
 * 
 * @author mikepersonick
 *
 */
public class BigdataGraphConfiguration implements GraphConfiguration {

    public interface Options {
        
        /**
         * Specify the type of bigdata instance to use - embedded or remote.
         */
        String TYPE = "properties.type";
        
        /**
         * Specifies that an embedded bigdata instance should be used.
         */
        String TYPE_EMBEDDED = "embedded";
        
        /**
         * Specifies that a remote bigdata instance should be used.
         */
        String TYPE_REMOTE = "remote";
    
        /**
         * Journal file for an embedded bigdata instance.
         */
        String FILE = "properties.file";
        
        /**
         * Host for a remote bigdata instance.
         */
        String HOST = "properties.host";
        
        /**
         * Port for a remote bigdata instance.
         */
        String PORT = "properties.port";
    
    }
    
    /**
     * Configure and return a BigdataGraph based on the supplied configuration
     * parameters.
     * 
     * @see {@link Options}
     * @see com.tinkerpop.rexster.config.GraphConfiguration#configureGraphInstance(com.tinkerpop.rexster.config.GraphConfigurationContext)
     */
    @Override
    public BigdataGraph configureGraphInstance(final GraphConfigurationContext context)
            throws GraphConfigurationException {
        
        try {
            
            return configure(context);
            
        } catch (Exception ex) {
            
            throw new GraphConfigurationException(ex);
            
        }
        
    }
    
    protected BigdataGraph configure(final GraphConfigurationContext context)
            throws Exception {
        
        final Configuration config = context.getProperties();
        
        if (!config.containsKey(Options.TYPE)) {
            throw new GraphConfigurationException("missing required parameter: " + Options.TYPE);
        }
        
        final String type = config.getString(Options.TYPE).toLowerCase();
        
        if (Options.TYPE_EMBEDDED.equals(type)) {
            
            if (config.containsKey(Options.FILE)) {
            
                final String journal = config.getString(Options.FILE);
            
                return BigdataGraphFactory.open(journal, true);
                
            } else {
                
                return BigdataGraphFactory.create();
                
            }
            
        } else if (Options.TYPE_REMOTE.equals(type)) {
            
            if (!config.containsKey(Options.HOST)) {
                throw new GraphConfigurationException("missing required parameter: " + Options.HOST);
            }
            
            if (!config.containsKey(Options.PORT)) {
                throw new GraphConfigurationException("missing required parameter: " + Options.PORT);
            }
            
            final String host = config.getString(Options.HOST);

            final int port = config.getInt(Options.PORT);
            
            return BigdataGraphFactory.connect(host, port);
            
        } else {
            
            throw new GraphConfigurationException("unrecognized value for "
                    + Options.TYPE + ": " + type);
            
        }
        
    }

}
