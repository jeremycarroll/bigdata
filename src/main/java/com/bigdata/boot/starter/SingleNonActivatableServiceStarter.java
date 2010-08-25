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
package com.bigdata.boot.starter;

import com.bigdata.util.config.ConfigDeployUtil;
import com.bigdata.util.config.ConfigurationUtil;
import com.bigdata.util.config.NicUtil;

import com.sun.jini.start.NonActivatableServiceDescriptor;
import com.sun.jini.start.ServiceDescriptor;
import net.jini.config.ConfigurationException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

/**
 * Convenience class that implements a boilerplate service starter
 * using Jini's NonActivatableServiceDescriptor to start exactly
 * one service. 
 *
 * The use of this class removes the need for a config file describing
 * just a single service; where the parameters that one would have 
 * previously defined in a config file for the service starter utility,
 * are instead defined using command line arguments and system properties.
 *
 * Command line arguments:
 *
 *   servicePolicyFile - The security policy file to use for the service.
 *   exportCodebase    - A colon-seperated list of downloadable JAR files
 *                       containing the classes that the service exports.
 *                       The JAR files listed must be downloadable through
 *                       the codebase server.
 *   importCodebase    - A classpath-like list of class locations for the
 *                       service implementation.
 *   serviceImplClass  - The name of the main class in the implementation.
 *   serviceImplArgs   - An optional list of arguments that will be passed
 *                       to the serviceImplClass' constructor.
 *
 * System properties:
 *
 *   bigdata.codebase.rootDir
 *       The directory that contains the jar files referred to by the
 *       exportCodebase parameter.
 *
 *   bigdata.codebase.port
 *       The port number of the codebase server on the codebase host.
 */
public class SingleNonActivatableServiceStarter extends ServiceStarter {

    public static void main(String args[]) {

        if(args.length < 4) {
            System.err.println("Usage: java [javaopts] com.bigdata.boot.starter.SingleNonActivatableServiceStarter <servicePolicyFile> <exportCodebase> <importCodebase> <serviceImplClass> [serviceImplArgs...]");

            System.err.println("");

            System.err.println("System Properties:");
            System.err.println("  bigdata.codebase.rootDir");
            System.err.println("  bigdata.codebase.port");
            System.err.println("");
            System.exit(1);
        }

        String servicePolicyFile = args[0];
        String exportCodebaseArg = args[1];
        String importCodebase = args[2];
        String serviceImplClassName = args[3];
        String[] serviceImplArgsArray = Arrays.copyOfRange(args,4,args.length);

        try {
            logger.log(Level.FINE, "creating service descriptor");

            String exportCodebase = 
                calculateExportCodebaseUrls(exportCodebaseArg);

            NonActivatableServiceDescriptor serviceDescriptor =
                new NonActivatableServiceDescriptor
                        (exportCodebase, servicePolicyFile, importCodebase,
                         serviceImplClassName, serviceImplArgsArray);

            ServiceStarter app = 
                new SingleNonActivatableServiceStarter
                    ( Arrays.asList((ServiceDescriptor)serviceDescriptor) );
            app.run();
            System.exit(0);
        } catch(Throwable t) {
            t.printStackTrace();
            System.exit(255);
        }
    }

    public SingleNonActivatableServiceStarter
               (List<ServiceDescriptor> serviceDescriptorList)
    {
        super(serviceDescriptorList);
    }

    /**
     * Convenience method that converts a ':' seperated list of Jar files
     * into a space-separated list of URLs. The URLs will be created
     * from the host and port values provided, respectively, by the
     * configuration entry with name 'node.serviceNetwork', and the
     * system property with name 'bigdata.codebase.port'.
     *
     * @throws IllegalArgumentException if the values of any of the required
     *         configuration items or system properties cannot be 
     *         successfully retrieved.
     *
     * @throws IOException if an I/O exception occurs while reading data
     *         when performing the codebase computation.
     */
    private static String calculateExportCodebaseUrls(String exportCodebase)
                              throws IOException
    {
        if( exportCodebase.isEmpty() ) return "";

        // Codebase server is run on the service network interface
        String nicName;
        try {
            nicName = ConfigDeployUtil.getString("node.serviceNetwork");
        } catch(ConfigurationException e) {
            throw new IllegalArgumentException
                          ("cannot determine codebase server address", e);
        }
        String codebaseHost = NicUtil.getIpAddress(nicName);

        String codebasePort = System.getProperty("bigdata.codebase.port");
        if(codebasePort == null) {
            throw new IllegalArgumentException
                      ("system property not set ['bigdata.codebase.port']");
        }

        String codebaseDir = System.getProperty("bigdata.codebase.rootDir");
        if(codebaseDir == null) {
            throw new IllegalArgumentException
                      ("system property not set ['bigdata.codebase.rootDir']");
        }
        String codebaseMD="none";

        String[] exportList = exportCodebase.split(":");
        String urlList = "";
        for(int i=0; i<exportList.length; i++) {
            String url = ConfigurationUtil.computeCodebase
                             (codebaseHost, exportList[i],
                              Integer.parseInt(codebasePort),
                              codebaseDir, codebaseMD);
            if(urlList.isEmpty()) {
                urlList = url;
            } else {
                urlList = urlList + " " + url;
            }
        }
        return urlList;
    }
}
