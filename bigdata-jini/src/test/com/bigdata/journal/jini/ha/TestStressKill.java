package com.bigdata.journal.jini.ha;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestStressKill extends TestCase {

    public static Test suite()
    {

        final TestSuite suite = new TestSuite("TestStressKill");

        // commitTime => (HALog|Snapshot)Record test suites.
        for (int i = 0; i < 100; i++) {
        	suite.addTestSuite(TestHA3JustKills.class);
        }
        
        return suite;
    }
   
}
