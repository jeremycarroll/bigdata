package com.bigdata.util.config;

import static org.junit.Assert.*;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class Log4jLoggingHandlerTest {

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
	public void testFlush() {
		Log4jLoggingHandler l = new Log4jLoggingHandler();
		l.flush();
	}

	@Test
	public void testClose() {
		Log4jLoggingHandler l = new Log4jLoggingHandler();
		l.close();
	}

	@Test
	public void testPublishLogRecord() {
		Log4jLoggingHandler l = new Log4jLoggingHandler();
		LogRecord rec = new LogRecord(Level.SEVERE, "Log message");
		l.publish(rec);
	}

	//TODO - test static logic
}
