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
//BTM import com.bigdata.service.jini.LoadBalancerServer;
import com.bigdata.util.NV;

//BTM
import com.bigdata.loadbalancer.EmbeddedLoadBalancer;

/**
BTM * Configuration for the {@link LoadBalancerServer}.
* Configuration for the {@link LoadBalancer} service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadBalancerConfiguration extends
        BigdataServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = 2815856971374520722L;

    /**
     * @param config
     */
    public LoadBalancerConfiguration(Configuration config)
            throws ConfigurationException {

//BTM        super(LoadBalancerServer.class, config);
super(com.bigdata.loadbalancer.ServiceImpl.class, config);
System.out.println("*** LoadBalancerConfiguration: constructor ***");
    }

    public LoadBalancerServiceStarter newServiceStarter(JiniFederation fed,
            IServiceListener listener, String zpath, Entry[] attributes)
            throws Exception {

System.out.println("*** LoadBalancerConfiguration ---> newServiceStarter ***");
        return new LoadBalancerServiceStarter(fed, listener, zpath, attributes);

    }

    public class LoadBalancerServiceStarter<V extends JiniServiceProcessHelper>
            extends BigdataServiceStarter<V> {

        /**
         * @param fed
         * @param listener
         * @param zpath
         */
        protected LoadBalancerServiceStarter(JiniFederation fed,
                IServiceListener listener, String zpath, Entry[] attributes) {

            super(fed, listener, zpath, attributes);
System.out.println("*** LoadBalancerConfiguration.LoadBalancerServiceStarter: constructor ***");

        }

        @Override
        protected NV getDataDir() {
            
System.out.println("*** LoadBalancerConfiguration.LoadBalancerServiceStarter: getDataDir [EmbeddedLoadBalancer.Options.LOG_DIR="+EmbeddedLoadBalancer.Options.LOG_DIR+", serviceDir="+serviceDir.toString()+"] ***");

//BTM            return new NV(LoadBalancerServer.Options.LOG_DIR, serviceDir
return new NV(EmbeddedLoadBalancer.Options.LOG_DIR, serviceDir
                    .toString());
            
        }
        
    }

}
