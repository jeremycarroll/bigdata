package com.bigdata.util.config;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class LogUtilTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetLog4jLoggerString() {
        Logger l = LogUtil.getLog4jLogger(LogUtilTest.class);
        assertNotNull(l);
        assertEquals(LogUtilTest.class.getName(), l.getName());
	}

	@Test
	public void testGetLog4jLoggerClass() {
        Logger l = LogUtil.getLog4jLogger(LogUtilTest.class.getName());
        assertNotNull(l);
        assertEquals(LogUtilTest.class.getName(), l.getName());
	}

	@Test
	public void testGetLog4jRootLogger() {
        Logger l = LogUtil.getLog4jRootLogger();
        assertNotNull(l);
        assertEquals("root", l.getName());
	}

	//TODO - Still need to test static property logic
}
