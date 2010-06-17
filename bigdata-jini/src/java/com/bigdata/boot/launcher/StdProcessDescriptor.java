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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

class StdProcessDescriptor extends BaseProcessDescriptor {

    public StdProcessDescriptor(LauncherMain         launcher,
                                ProcessConfiguration config)
    {
        super(launcher, config);
        if( !("std".equals(config.cls)) ) {
            throw new IllegalStateException("invalid process class");
        }
    }

    protected Process spawnProcess() throws IOException {

        ProcessBuilder pbuilder = new ProcessBuilder();
        List<String> argList = new java.util.ArrayList<String>();
        pbuilder.redirectErrorStream(true);

        Map<String,String> configProps = config.properties.getProperties();

        // Add the environment variables to the ProcessBuilder environment
        Map<String, String> env = pbuilder.environment();
        for(Map.Entry<String,String> ent :
                                     config.properties.getEnvs().entrySet())
        {
            env.put(ent.getKey(), ent.getValue());
        }

        env.put("BOOT_TAG", this.tag);
        env.put("BOOT_PORT", configProps.get("boot.port"));

        // Add the executable name
        if( !(configProps.containsKey("app.executable")) ) {
            throw new IllegalStateException
                          ("missing property [name='app.executable']");
        }
        argList.add(configProps.get("app.executable"));

        // Add command line arguments
        argList.addAll(config.args);

        // Command line argument quoting on Windows is broken. Work around
        // it here by quoting the arguments if running on Windows.
        if( "windows".equals(configProps.get("os.name")) ) {
            argList = quoteWindowsArguments(argList);
        }

        // Log the command that's about to be run
        logger.log(Level.FINE, "executing a process {0}", argList);

        pbuilder.command(argList);
        return pbuilder.start();
    }
}
