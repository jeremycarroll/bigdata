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
package com.bigdata.boot.launcher;

import com.bigdata.boot.ProcessState;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

class JavaProcessDescriptor extends BaseProcessDescriptor {

    public JavaProcessDescriptor(LauncherMain         launcher,
                                 ProcessConfiguration config)
    {
        super(launcher, config);
        if( !("java".equals(config.cls)) )
            throw new IllegalStateException("invalid process class");
    }

    protected Process spawnProcess() throws IOException {
        logger.info("spawningProcess in dir "+new File("").getAbsolutePath() );
        logger.info("props are : " + config.properties.getProperties().toString());
        ProcessBuilder pbuilder = new ProcessBuilder();
        List<String> argList = new ArrayList<String>();
        pbuilder.redirectErrorStream(true);

        Map<String,String> configProps = config.properties.getProperties();

        // Add the environment variables to the ProcessBuilder environment
        Map<String, String> env = pbuilder.environment();
        for( Map.Entry<String,String> ent :
                                      config.properties.getEnvs().entrySet() )
        {
            env.put(ent.getKey(), ent.getValue());
        }

        if( configProps.containsKey("java.command") ) {
            argList.add(configProps.get("java.command"));
        } else {
            argList.add("java");//let PATH env variable sort it out
        }

        if( configProps.containsKey("java.initmemory") ) {
            argList.add("-Xms" + configProps.get("java.initmemory"));
        }

        if( configProps.containsKey("java.maxmemory") ) {
            argList.add("-Xmx" + configProps.get("java.maxmemory"));
        }

        if( configProps.containsKey("java.classpath") ) {
            argList.add("-cp");
            argList.add(configProps.get("java.classpath"));
        }

        // Add java command arguments to the arg list (java.args.*)
        for(int i=1; true; i++) {
            String argVal = configProps.get("java.args." + i);
            if(argVal == null) break;
            argList.add(argVal);
        }

        // Add the system property definitions as command line arguments
        argList.add("-Dboot.processtag=" + this.tag);
        argList.add("-Dboot.port=" + configProps.get("boot.port"));

        for( Map.Entry<String,String> ent :
                                config.properties.getJavaProps().entrySet() )
        {
            argList.add("-D" + ent.getKey() + "=" + ent.getValue());
        }

        // Get the application's main class
        if( !(configProps.containsKey("java.app.mainclass")) ) {
            throw new IllegalStateException
                          ("missing property [name='java.app.mainclass']");
        }
        argList.add(configProps.get("java.app.mainclass"));

        // Add arguments to the main class
        argList.addAll(config.args);

        // Command line argument quoting in Java on Windows is broken.
        // Work around it here by quoting the arguments if running on Windows.
        if( "windows".equals(configProps.get("os.name")) ) {
            argList = quoteWindowsArguments(argList);
        }

        // Log the command that's about to be executed
        logger.log(Level.FINE, "executing a process {0}", argList);

        pbuilder.command(argList);//execute the new process
        return pbuilder.start();
    }
}
