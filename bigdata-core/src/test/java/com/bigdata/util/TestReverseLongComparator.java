package com.bigdata.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestReverseLongComparator {

	public TestReverseLongComparator() {
		super();
	}

	// Note: reverse comparison --> reverse of compare contract
    @Test
	public void testCompare_non_negative() {
		ReverseLongComparator comp = new ReverseLongComparator();
		assertTrue(comp.compare(1L, 2L) > 0);
		assertTrue(comp.compare(1L, 1L) == 0);
		assertTrue(comp.compare(1L, 0L) < 1);
	}
    @Test
	public void testCompare_negative() {
		ReverseLongComparator comp = new ReverseLongComparator();
		assertTrue(comp.compare(-2L, -1L) > 0);
		assertTrue(comp.compare(-1L, -1L) == 0);
		assertTrue(comp.compare(0L, -1L) < 1);
	}

}
