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
 * Created on Jan 5, 2009
 */

package com.bigdata.jini.start.config;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.entry.Entry;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.process.JiniServiceProcessHelper;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.MetadataServer;
import com.bigdata.util.NV;

//BTM
import com.bigdata.metadata.EmbeddedShardLocator;

/**
 * Configuration for the {@link MetadataServer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MetadataServerConfiguration extends
        BigdataServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = -3476429607652513836L;

    /**
     * @param config
     */
//BTM - BEGIN
//BTM    public MetadataServerConfiguration(Configuration config)
//BTM            throws ConfigurationException {
//BTM
//BTM        super(MetadataServer.class, config);
//BTM
//BTM    }
    public MetadataServerConfiguration(Class         classType,
                                       Configuration config)
               throws ConfigurationException
    {
        super(classType, config);
System.out.println("*** MetadataServerConfiguration: constructor ***");
    }
//BTM - END

    public MetadataServiceStarter newServiceStarter(JiniFederation fed,
            IServiceListener listener, String zpath, Entry[] attributes)
            throws Exception {

System.out.println("*** MetadataServerConfiguration ---> newServiceStarter ***");
        return new MetadataServiceStarter(fed, listener, zpath, attributes);

    }

    public class MetadataServiceStarter<V extends JiniServiceProcessHelper>
            extends BigdataServiceStarter<V> {

        /**
         * @param fed
         * @param listener
         * @param zpath
         */
        protected MetadataServiceStarter(JiniFederation fed,
                IServiceListener listener, String zpath, Entry[] attributes) {

            super(fed, listener, zpath, attributes);
System.out.println("*** MetadataServerConfiguration.MetadataServiceStarter: constructor ***");

        }

        @Override
        protected NV getDataDir() {
            
//BTM            return new NV(MetadataServer.Options.DATA_DIR, serviceDir
//BTM                    .toString());
//BTM - BEGIN
            // className field defined/set in ServiceConfiguration parent
            if ( (MetadataServer.class.getName()).equals(className) ) {
System.out.println("*** MetadataConfiguration.MetadataServiceStarter: getDataDir [MetadataServer.Options.DATA_DIR="+MetadataServer.Options.DATA_DIR+", serviceDir="+serviceDir.toString()+"] ***");
                return new NV(MetadataServer.Options.DATA_DIR, serviceDir.toString());
            } else if ( (com.bigdata.loadbalancer.ServiceImpl.class.getName()).equals(className) ) {
System.out.println("*** MetadataConfiguration.MetadataServiceStarter: getDataDir [EmbeddedShardLocator.Options.DATA_DIR="+EmbeddedShardLocator.Options.DATA_DIR+", serviceDir="+serviceDir.toString()+"] ***");
                return new NV(EmbeddedShardLocator.Options.DATA_DIR, serviceDir.toString());
            } else {
                return null;
            }
//BTM - END
            
        }
        
    }

}
