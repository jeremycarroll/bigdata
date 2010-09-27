package com.bigdata.util;

import junit.framework.TestCase;

public class TestNV extends TestCase {


	public void testGetName() {
		String name =  "ABC";
	    NV nt = new NV(name, name);
	    assertEquals(name, nt.getName());
	}

	public void testGetValue() {
		String name =  "ABC";
	    NV nt = new NV(name, name);
	    assertEquals(name, nt.getValue());
	}

	public void testNV_null() {
	    try {
	    	new NV(null, "abc");
	    	fail("Successfully called NV() with null name argument.");
	    } catch (IllegalArgumentException e) {
	    	//ignore -- expected 
	    }
	}

	public void testToString() {
    	String name = "Name";
    	String value = "Value";
    	NV nv = new NV(name, value);
    	assertTrue((name+"="+value).equals(nv.toString()));
	}

	public void testEqualsNV() {
		
    	String name = "abc";
    	String value = "xyz";
    	NV h = new NV(name, value);
    	NV h_dup = new NV(name, value);
    	NV h_dup2 = new NV(name, value);
    	NV h_diff = new NV(name + "diff", value);
    	NV h_diff2 = new NV(name, (value + "diff"));
    	
    	// Test reflexive property
    	assertTrue(h.equals(h));
    	
    	// Test symmetric property    	
    	assertTrue(h.equals(h_dup) && h_dup.equals(h));
    	
    	//Test transitive property
    	assertTrue(h.equals(h_dup) && h_dup.equals(h_dup2) && h.equals(h_dup2));
    	
    	// consistency property already tested
    	
    	// Test negative cases
    	assertFalse(h.equals(null));
    	
    	assertFalse(h.equals(name));
    	
    	assertFalse(h.equals(h_diff));
    	assertFalse(h.equals(h_diff2));	
    }
	
	public void testHashCode() {
    	String name = "abc";
    	String value = "xyz";
    	NV h = new NV(name, value);
    	NV h_dup = new NV(name, value);
    	assertTrue(h.hashCode()==h_dup.hashCode());
	}

	public void testCompareTo() {
    	String name = "bcd";
    	String value = "xyz";
    	String lessThanName = "abc";
    	String greaterThanName = "cde";
    	NV nv = new NV(name, value);
    	NV ltnv = new NV(lessThanName, value);
    	NV gtnv = new NV(greaterThanName, value);
    	assertTrue(nv.compareTo(nv) == 0);
    	assertTrue(ltnv.compareTo(nv) < 0);
    	assertTrue(gtnv.compareTo(nv) > 0);
	}
	
	public void testCompareContract() {
		/*
		 * The NV class has a natural ordering that is inconsistent with equals.
		 * When equals and compare align, need to switch this test and the note, above,
		 * in the NV source code.
		 */ 
    	String name = "bcd";
    	String value1 = "xyz";
    	String value2 = "lmn";
    	NV nv = new NV(name, value1);
    	NV nv_diff_val = new NV(name, value2);
    	assertFalse(nv.equals(nv_diff_val));
		assertTrue(nv.compareTo(nv_diff_val)==0);
	}

}
