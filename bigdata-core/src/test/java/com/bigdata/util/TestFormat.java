package com.bigdata.util;

import junit.framework.TestCase;

public class TestFormat extends TestCase {

	public TestFormat(String name) {
		super(name);
	}

	public void testFormat() {
		String fmtString = "Roses are {0}, violets are {1}";
		Object[] fmtArgs = new Object[] { "red", "blue" };
		Format f = new Format(fmtString, fmtArgs);
		assertTrue("Roses are red, violets are blue".equals(f.toString()));
	}
}
