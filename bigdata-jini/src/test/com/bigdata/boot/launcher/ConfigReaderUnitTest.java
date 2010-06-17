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

package com.bigdata.boot.launcher;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.util.config.LogUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.File;
import java.util.Collection;

public class ConfigReaderUnitTest extends TestCase {

    private static String F_SEP    = System.getProperty("file.separator");
    private static String ROOT_DIR = "bigdata-jini"
                                     +F_SEP+"src"
                                     +F_SEP+"test"
                                     +F_SEP+"com"
                                     +F_SEP+"bigdata"
                                     +F_SEP+"boot"
                                     +F_SEP+"launcher"
                                     +F_SEP+"config";
    private String  testName;
    private boolean testPassed;

    private static Logger logger;

    // NOTE: remove constructors and tearDown when/if the junit infrastructure
    //       is upgraded to version that supports annotations and this test
    //       is changed so that it no longer has to extend TestCase.

    public ConfigReaderUnitTest() {
        initAll();
    }

    public ConfigReaderUnitTest(String name) {
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
                            ( (ConfigReaderUnitTest.class).getName() );
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

    // File structural tests

    @Test
    public void testEmptyFileParse() throws Exception {

        testName = "testEmptyFileParseTest";
        testPassed = false;

        ConfigReader rdr = new ConfigReader
                               ( new File(ROOT_DIR+F_SEP+"empty.xml"),
                                 new ConfigurationPropertyMap() );

        ConfigurationPropertyMap propMap = rdr.getBootPropertyMap();

        assertTrue("properties not empty", propMap.getProperties().isEmpty());
        assertTrue("javaprops not empty", propMap.getJavaProps().isEmpty());
        assertTrue("envs not empty", propMap.getEnvs().isEmpty());

        assertTrue("process list not empty", rdr.getProcessList().isEmpty());
        assertTrue("boot process list not empty",
                   rdr.getBootProcesses().isEmpty());

        testPassed = true;
    }

    @Test(expected=IOException.class)
    public void testNonexistantFile() throws Exception {
        testName = "testNonexistantFileTest";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                                   (new File(ROOT_DIR+F_SEP+"nonexistent.xml"),
                                    new ConfigurationPropertyMap());
        } catch(IOException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testMissingBootTag() throws Exception {
        testName = "testmissingBootTagTest";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                               (new File(ROOT_DIR+F_SEP+"missingBootTag.xml"),
                                new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidTag() throws Exception {
        testName = "testinvalidTagTest";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                                   (new File(ROOT_DIR+F_SEP+"invalidTag.xml"),
                                    new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_boot_arg() throws Exception {
        testName = "testinvalidNesting_boot_arg";
        testPassed = false;
        try {
            ConfigReader rdr = 
            new ConfigReader
                    (new File(ROOT_DIR+F_SEP+"invalidNesting_boot_arg.xml"),
                     new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_boot_boot() throws Exception {
        testName = "testinvalidNesting_boot_boot";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                (new File(ROOT_DIR+F_SEP+"invalidNesting_boot_boot.xml"),
                 new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_javaprop_arg() throws Exception {
        testName = "testInvalidNesting_javaprop_arg";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                (new File(ROOT_DIR+F_SEP+"invalidNesting_javaprop_arg.xml"),
                 new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_javaprop_boot() throws Exception {
        testName = "testInvalidNesting_javaprop_boot";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                (new File(ROOT_DIR+F_SEP+"invalidNesting_javaprop_boot.xml"),
                 new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_javaprop_javaprop() throws Exception {
        testName = "testInvalidNesting_javaprop_javapro";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
              (new File(ROOT_DIR+F_SEP+"invalidNesting_javaprop_javaprop.xml"),
               new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_javaprop_process() throws Exception {
        testName = "testInvalidNesting_javaprop_process";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
              (new File(ROOT_DIR+F_SEP+"invalidNesting_javaprop_process.xml"),
               new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_javaprop_property() throws Exception {
        testName = "testInvalidNesting_javaprop_property";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
              (new File(ROOT_DIR+F_SEP+"invalidNesting_javaprop_property.xml"),
               new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_process_boot() throws Exception {
        testName = "testInvalidNesting_process_boot";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                (new File(ROOT_DIR+F_SEP+"invalidNesting_process_boot.xml"),
                 new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_property_arg() throws Exception {
        testName = "testInvalidNesting_property_arg";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                (new File(ROOT_DIR+F_SEP+"invalidNesting_property_arg.xml"),
                 new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_property_boot() throws Exception {
        testName = "testInvalidNesting_property_boot";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                (new File(ROOT_DIR+F_SEP+"invalidNesting_property_boot.xml"),
                 new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_property_javaprop() throws Exception {
        testName = "testInvalidNesting_property_javaprop";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
              (new File(ROOT_DIR+F_SEP+"invalidNesting_property_javaprop.xml"),
               new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_property_process() throws Exception {
        testName = "testInvalidNesting_property_process";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
               (new File(ROOT_DIR+F_SEP+"invalidNesting_property_process.xml"),
                new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidNesting_property_property() throws Exception {
        testName = "testInvalidNesting_property_property";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
              (new File(ROOT_DIR+F_SEP+"invalidNesting_property_property.xml"),
               new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    // Property tag tests

    @Test
    public void testPropertyTag() throws Exception {
        testName = "testPropertyTagTest";
        testPassed = false;
        ConfigReader rdr = 
            new ConfigReader(new File(ROOT_DIR+F_SEP+"propertytest.xml"),
                             new ConfigurationPropertyMap());
        ConfigurationPropertyMap propMap = rdr.getBootPropertyMap();
        assertEquals
            ("thisisaVALUE 123", propMap.getProperties().get("val123Prop"));
        testPassed = true;
    }

    @Test
    public void testJavapropTag() throws Exception {
        testName = "testJavapropTagTest";
        testPassed = false;
        ConfigReader rdr = 
            new ConfigReader(new File(ROOT_DIR+F_SEP+"propertytest.xml"),
                             new ConfigurationPropertyMap());
        ConfigurationPropertyMap propMap = rdr.getBootPropertyMap();
        assertEquals
            ("fooPolicyVal", propMap.getJavaProps().get("java.foo.policy"));
        testPassed = true;
    }

    @Test
    public void testEnvVar() throws Exception {
        testName = "testEnvVarTest";
        testPassed = false;
        ConfigReader rdr = 
            new ConfigReader(new File(ROOT_DIR+F_SEP+"propertytest.xml"),
                             new ConfigurationPropertyMap());
        ConfigurationPropertyMap propMap = rdr.getBootPropertyMap();
        assertEquals("/opt/bigdata", propMap.getEnvs().get("BIGDATA_HOME"));
        testPassed = true;
    }

    @Test
    public void testPropertySubstitution() throws Exception {
        testName = "testPropertySubstitutionTest";
        testPassed = false;
        ConfigReader rdr = 
            new ConfigReader(new File(ROOT_DIR+F_SEP+"propertytest.xml"),
                             new ConfigurationPropertyMap());
        ConfigurationPropertyMap propMap = rdr.getBootPropertyMap();
        assertEquals
            ("xXthisisaVALUE 123Xx", propMap.getProperties().get("lisa"));
        testPassed = true;
    }

// Not possible to test? In the current environment it appears it's
// not possible to set environment variables for the junit tests.
//    // This test requires the environment variable "TESTENVAR" to be
//    // set to "Doh".
//    @Test
//    public void testEnvVarSubstitution() throws Exception {
//        testName = "testenvVarSubstitutionTest";
//        testPassed = false;
//        ConfigReader rdr = 
//            new ConfigReader(new File(ROOT_DIR+F_SEP+"propertytest.xml"),
//                             new ConfigurationPropertyMap());
//        ConfigurationPropertyMap propMap = rdr.getBootPropertyMap();
//        assertEquals("Doh", propMap.getProperties().get("homer"));
//        testPassed = true;
//    }


    @Test
    public void testPropertyOverride() throws Exception {
        testName = "testPropertyOverrideTest";
        testPassed = false;
        ConfigReader rdr = 
            new ConfigReader(new File(ROOT_DIR+F_SEP+"propertytest.xml"),
                             new ConfigurationPropertyMap());
        ConfigurationPropertyMap propMap = rdr.getBootPropertyMap();
        assertEquals("THE_NEW_VALUE", propMap.getProperties().get("bart"));
        testPassed = true;
    }


    // TODO: Read config file referencing an undefined property
    // TODO: Read config file referencing an undefined env variable


    // File inclusion tests

    @Test
    public void testIncludeFile() throws Exception {
        testName = "testIncludeFileTest";
        testPassed = false;
        ConfigReader rdr = new ConfigReader
                               (new File(ROOT_DIR+F_SEP+"includeFile.xml"),
                                new ConfigurationPropertyMap());
        ConfigurationPropertyMap propMap = rdr.getBootPropertyMap();
        assertEquals("okay", propMap.getProperties().get("filewasincluded"));
        testPassed = true;
    }

    @Test
    public void testIncludeNonexistentFile() throws Exception {
        testName = "testIncludeNonexistentFileTest";
        testPassed = false;
        ConfigReader rdr = new ConfigReader
            (new File(ROOT_DIR+F_SEP+"includeNonexistentFile.xml"),
             new ConfigurationPropertyMap());
        ConfigurationPropertyMap propMap = rdr.getBootPropertyMap();
        assertTrue
            ("properties is not empty", propMap.getProperties().isEmpty());
        testPassed = true;
    }

    @Test(expected=IOException.class)
    public void testIncludeNonexistentRequiredFile() throws Exception {
        testName = "testIncludeNonexistentRequiredFileTest";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                (new File(ROOT_DIR+F_SEP+"includeNonexistentRequiredFile.xml"),
                 new ConfigurationPropertyMap());
        } catch(IOException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    // <process> tag tests

    @Test
    public void testProcessTag() throws Exception {
        testName = "testProcessTagTest";
        testPassed = false;
        ConfigReader rdr = 
            new ConfigReader(new File(ROOT_DIR+F_SEP+"processTagTest.xml"),
                             new ConfigurationPropertyMap());

        Collection<ProcessConfiguration> processConfigs = rdr.getProcessList();
        assertEquals(2, processConfigs.size());
        for(ProcessConfiguration c : processConfigs) {

            if( c.tag.equals("testproc1") ) {

                assertEquals("std1", c.cls);
                assertEquals(2, c.args.size());
                assertEquals("--junk1", c.args.get(0));
                assertEquals("--verbose", c.args.get(1));
                assertEquals("commonPropVal",
                             c.properties.getProperties().get("commonProp"));
                assertEquals
                    ("inTestProc1",
                     c.properties.getProperties().get("overriddenProp"));

            } else if( c.tag.equals("testproc2") ) {

                assertEquals("std2", c.cls);
                assertEquals(2, c.args.size());
                assertEquals("--quiet", c.args.get(0));
                assertEquals("--junk2", c.args.get(1));
                assertEquals("commonPropVal",
                             c.properties.getProperties().get("commonProp"));
                assertEquals
                    ("valueForTestProc2",
                     c.properties.getProperties().get("overriddenProp"));

            } else {
                fail("unknown process tag ['"+c.tag+"']");
            }
        }
        testPassed = true;
    }

    @Test(expected=SAXException.class)
    public void testInvalidProcessTag_missingTag() throws Exception {
        testName = "testInvalidProcessTag_missingTag";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                (new File(ROOT_DIR+F_SEP+"invalidProcessTag_missingTag.xml"),
                 new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(expected=SAXException.class)
    public void testInvalidProcessTag_missingClass() throws Exception {
        testName = "testInvalidProcessTag_missingClass";
        testPassed = false;
        try {
            ConfigReader rdr = new ConfigReader
                (new File(ROOT_DIR+F_SEP+"invalidProcessTag_missingClass.xml"),
                 new ConfigurationPropertyMap());
        } catch(SAXException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    // TODO: tests for <process> defs
    //  * Attempt to define 2 procs with same tag
    //  * put process tag inside a process tag (negative test)
}
