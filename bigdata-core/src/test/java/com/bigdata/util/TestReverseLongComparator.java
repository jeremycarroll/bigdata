package com.bigdata.util;

import junit.framework.TestCase;

public class TestReverseLongComparator extends TestCase {

	public TestReverseLongComparator(String name) {
		super(name);
	}

	// Note: reverse comparison --> reverse of compare contract
	public void testCompare_non_negative() {
		ReverseLongComparator comp = new ReverseLongComparator();
		assertTrue(comp.compare(1L, 2L) > 0);
		assertTrue(comp.compare(1L, 1L) == 0);
		assertTrue(comp.compare(1L, 0L) < 1);
	}

	public void testCompare_negative() {
		ReverseLongComparator comp = new ReverseLongComparator();
		assertTrue(comp.compare(-2L, -1L) > 0);
		assertTrue(comp.compare(-1L, -1L) == 0);
		assertTrue(comp.compare(0L, -1L) < 1);
	}

}
