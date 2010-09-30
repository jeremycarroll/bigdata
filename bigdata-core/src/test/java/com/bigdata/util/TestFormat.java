package com.bigdata.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestFormat {

	public TestFormat() {
		super();
	}
    @Test
	public void testFormat() {
		String fmtString = "Roses are {0}, violets are {1}";
		Object[] fmtArgs = new Object[] { "red", "blue" };
		Format f = new Format(fmtString, fmtArgs);
		assertTrue("Roses are red, violets are blue".equals(f.toString()));
	}
}
