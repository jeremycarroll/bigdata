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

package com.bigdata.process;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import static com.bigdata.process.Constants.*;
import com.bigdata.util.Format;
import com.bigdata.util.config.LogUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

public class ProcessConfigXmlHandlerTest extends TestCase {

    private static String F_SEP = System.getProperty("file.separator");
    private static String CONFIG_FILE = "bigdata-jini"
                                        +F_SEP+"src"
                                        +F_SEP+"java"
                                        +F_SEP+"com"
                                        +F_SEP+"bigdata"
                                        +F_SEP+"process"
                                        +F_SEP+"config"
                                        +F_SEP+"process-definitions.xml";

    private static Map<ProcessInfo, RoleInfo> rolesMap =
                       new TreeMap<ProcessInfo, RoleInfo>();

    private Map<String, ProcessManagement> processes;
    private Map<ProcessInfo, RoleInfo> roles;

    private String  testName;
    private boolean testPassed;

    private static Logger logger;

    // NOTE: remove constructors and tearDown when/if the junit infrastructure
    //       is upgraded to version that supports annotations and this test
    //       is changed so that it no longer has to extend TestCase.

    public ProcessConfigXmlHandlerTest() {
        initAll();
    }

    public ProcessConfigXmlHandlerTest(String name) {
        super(name);
        initAll();
    }

    public void tearDown() throws Exception {
        cleanUp();
    }

    @BeforeClass public static void initAll() {
        String logConfigFile = "ant-build"
                               +F_SEP+"classes"
                               +F_SEP+"test"
                               +F_SEP+"resources"
                               +F_SEP+"logging"
                               +F_SEP+"log4j.properties";
        System.setProperty("log4j.configuration", logConfigFile);
        logger = LogUtil.getLog4jLogger
                            ( (ProcessConfigXmlHandlerTest.class).getName() );
        logger.log(Level.INFO,
                   "--------------------------------------------------");

        ProcessInfo pInfo;
        RoleInfo    rInfo;

        // type=standalone, layout=1-of-1

        // role=bigdata, processes=lookup, quorum,
        //                         loadbalancer, shardlocator, transaction,
        //                         executor, shard
        String type   = "standalone";
        String layout = "1-of-1";
        for(int i=0; i<NODE_ROLES.length; i++) {
            String role = NODE_ROLES[i];
            String[] procByRole;
            if( "bigdata".equals(role) ) {
                procByRole = new String[] {"lookup", "quorum",
                                           "loadbalancer", "shardlocator",
                                           "transaction","executor", "shard" };
            } else if( "binary".equals(role) ) {
continue;//TODO
            } else {
                continue;
            }
            pInfo = new ProcessInfo(type, layout, role);
            rInfo = new RoleInfo(pInfo);
            for(int j=0; j<procByRole.length; j++) {
                rInfo.addProcess(procByRole[j]);
            }
            rolesMap.put(pInfo, rInfo);
        }

        // type=storage, layout=*

        // rolerole=bigdata, processes=shard
        for(int i=0; i<NODE_LAYOUTS.length; i++) {
            pInfo = new ProcessInfo("storage", NODE_LAYOUTS[i], "bigdata");
            rInfo = new RoleInfo(pInfo);
            rInfo.addProcess("shard");
            rolesMap.put(pInfo, rInfo);
        }

        // type=infrastructure, layout=1-of-1

        // role=bigdata, processes=lookup, quorum, executor, loadbalancer,
        //                         shardlocator, transaction
        pInfo = new ProcessInfo("infrastructure", "1-of-1", "bigdata");
        rInfo = new RoleInfo(pInfo);
        rInfo.addProcess("lookup");
        rInfo.addProcess("quorum");
        rInfo.addProcess("loadbalancer");
        rInfo.addProcess("shardlocator");
        rInfo.addProcess("transaction");
        rInfo.addProcess("executor");
        rolesMap.put(pInfo, rInfo);

        // type=infrastructure, layout=1-of-3

        // role=bigdata, processes=lookup, quorum, loadbalancer
        pInfo = new ProcessInfo("infrastructure", "1-of-3", "bigdata");
        rInfo = new RoleInfo(pInfo);
        rInfo.addProcess("lookup");
        rInfo.addProcess("quorum");
        rInfo.addProcess("loadbalancer");
        rolesMap.put(pInfo, rInfo);

        // type=infrastructure, layout=2-of-3

        // role=bigdata, processes=lookup, quorum, executor, shardlocator
        pInfo = new ProcessInfo("infrastructure", "2-of-3", "bigdata");
        rInfo = new RoleInfo(pInfo);
        rInfo.addProcess("lookup");
        rInfo.addProcess("quorum");
        rInfo.addProcess("shardlocator");
        rInfo.addProcess("executor");
        rolesMap.put(pInfo, rInfo);

        // type=infrastructure, layout=3-of-3

        // role=bigdata, processes=quorum, loadbalancer, transaction
        pInfo = new ProcessInfo("infrastructure", "3-of-3", "bigdata");
        rInfo = new RoleInfo(pInfo);
        rInfo.addProcess("quorum");
        rInfo.addProcess("transaction");
        rInfo.addProcess("loadbalancer");
        rolesMap.put(pInfo, rInfo);

        // type=infrastructure, layout=1-of-5

        // role=bigdata, processes=lookup, quorum, loadbalancer
        pInfo = new ProcessInfo("infrastructure", "1-of-5", "bigdata");
        rInfo = new RoleInfo(pInfo);
        rInfo.addProcess("lookup");
        rInfo.addProcess("quorum");
        rInfo.addProcess("loadbalancer");
        rolesMap.put(pInfo, rInfo);

        // role=bigdata, processes=lookup, quorum, shardlocator
        pInfo = new ProcessInfo("infrastructure", "2-of-5", "bigdata");
        rInfo = new RoleInfo(pInfo);
        rInfo.addProcess("lookup");
        rInfo.addProcess("quorum");
        rInfo.addProcess("shardlocator");
        rolesMap.put(pInfo, rInfo);

        // role=bigdata, processes=quorum, transaction
        pInfo = new ProcessInfo("infrastructure", "3-of-5", "bigdata");
        rInfo = new RoleInfo(pInfo);
        rInfo.addProcess("quorum");
        rInfo.addProcess("transaction");
        rolesMap.put(pInfo, rInfo);

        // role=bigdata, processes=quorum, executor
        pInfo = new ProcessInfo("infrastructure", "4-of-5", "bigdata");
        rInfo = new RoleInfo(pInfo);
        rInfo.addProcess("quorum");
        rInfo.addProcess("executor");
        rolesMap.put(pInfo, rInfo);

        // role=bigdata, processes=quorum, executor
        pInfo = new ProcessInfo("infrastructure", "5-of-5", "bigdata");
        rInfo = new RoleInfo(pInfo);
        rInfo.addProcess("quorum");
        rInfo.addProcess("executor");
        rolesMap.put(pInfo, rInfo);
    }

    @After public void cleanUp() {

        if(testName != null) {
            String prefix = (testPassed ? "PASSED: " : "FAILED: ");
            logger.log(Level.INFO, prefix+testName);
            logger.log(Level.INFO,
                       "--------------------------------------------------");
        } else {
            logger.log(Level.INFO, " ");
        }
    }

    private ProcessConfigXmlHandler parseDefs() throws Exception {
        processes = new TreeMap<String, ProcessManagement>();
        roles = new TreeMap<ProcessInfo, RoleInfo>();

        ProcessConfigXmlHandler configHandler =
                                new ProcessConfigXmlHandler(processes, roles);
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            saxParser.parse(CONFIG_FILE, configHandler);
        } catch(Exception e) {
            fail("exception parsing XML config "
                 +"[file="+CONFIG_FILE+", exception="+e+"]");
        }
        return configHandler;
    }


    // File structural tests

    @Test
    public void testParseDefsVerifyRoles() throws Exception {

        testName = "testParseDefsVerifyRolesTest";
        testPassed = false;

        parseDefs();

        for(RoleInfo rInfo : roles.values()) {
            logger.log(Level.DEBUG, rInfo);
        }

        Collection<RoleInfo> parsedRoles = roles.values();
        Collection<RoleInfo> expectedRoles = rolesMap.values();

        assertTrue("expecteded roles does not contain all of the parsed roles",
                   expectedRoles.containsAll(parsedRoles) );
        assertTrue("parsed roles does not contain all of the expected roles",
                   parsedRoles.containsAll(expectedRoles) );
        testPassed = true;
    }

    @Test
    public void testParseDefsVerifyProcesses() throws Exception {

        testName = "testParseDefsVerifyProcessesTest";
        testPassed = false;

        parseDefs();

        for(RoleInfo rInfo : roles.values()) {
            for(String pTag : rInfo.getProcessSet()) {
                if( !processes.containsKey(pTag) ) {
                    fail("process not defined [type="+rInfo.getType()
                         +", layout="+rInfo.getLayout()
                         +", role="+rInfo.getRole()+", process='"+pTag+"']");
                }
            }
        }
        testPassed = true;
    }

    @Test
    public void testParseDefsVerifyRestartGroups() throws Exception {

        testName = "testParseDefsVerifyRestartGroupsTest";
        testPassed = false;

        ProcessConfigXmlHandler configHandler = parseDefs();

        List<Set<String>> parsedRestartList = configHandler.getRestartGroups();
        for(Set<String> memberSet : parsedRestartList) {
            RestartGroup rGroup = new RestartGroup();
            for(String pTag : memberSet) {
                ProcessManagement pInfo = processes.get(pTag);
                if(pInfo == null) {
                    fail("unknown process for restart [process='"+pTag+"']");
                }
                rGroup.addMember(pInfo);
                pInfo.setRestartGroup(rGroup);
            }
            logger.log(Level.DEBUG, new Format("restart group {0}", rGroup) );
        }

        List<Set<String>> expectedRestartList = new ArrayList<Set<String>>();

        Set<String> expectedRestartProcs1 = new TreeSet<String>();
//TODO        expectedRestartProcs1.add("process-A");
//TODO        expectedRestartProcs1.add("process-B");
//TODO        expectedRestartProcs1.add("process-C");

//TODI        expectedRestartList.add( expectedRestartProcs1 );

        assertTrue("expecteded list of restart group sets does not contain "
                   +"all of the restart group sets from the parsed list",
                   expectedRestartList.containsAll(parsedRestartList) );
        assertTrue("parsed list of restart groups does not contain all of "
                   +"the expected restart group sets",
                   parsedRestartList.containsAll(expectedRestartList) );

        testPassed = true;
    }

    @Test
    public void testParseDefsVerifyNodeConfig() throws Exception {

        testName = "testParseDefsVerifyRestartGroupsTest";
        testPassed = false;

        parseDefs();

        String[] nodeTypes = new String[] { "infrastructure" };
        String[] nodeLayouts = new String[] { "1-of-3" };
        String[] nodeRoles = new String[] { "bigdata" };
        for(String type : nodeTypes) {
            for(String layout : nodeLayouts) {
                for(String role : nodeRoles) {
                    ProcessInfo pInfo = new ProcessInfo(type, layout, role);
                    RoleInfo rInfo = roles.get(pInfo);
                    if(rInfo == null) {
                        fail("no role info for configured process info - "
                             +pInfo);
                    }
                }
            }
        }

        nodeTypes = new String[] { "infrastructure", "storage" };
        nodeLayouts = new String[] { "1-of-3", "1-of-1" };
        nodeRoles = new String[] { "bigdata" };
        for(String type : nodeTypes) {
            for(String layout : nodeLayouts) {
                for(String role : nodeRoles) {
                    ProcessInfo pInfo = new ProcessInfo(type, layout, role);
                    RoleInfo rInfo = roles.get(pInfo);
                    if(rInfo == null) {
                        fail("no role info for configured process info - "
                             +pInfo);
                    }
                }
            }
        }

        testPassed = true;
    }
}
