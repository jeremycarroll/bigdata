package com.bigdata.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestNT {

    @Test
	public void testGetName() {
		String name =  "ABC";
	    NT nt = new NT(name, 1L);
	    assertEquals(name, nt.getName());
	}
    @Test
	public void testGetTimestamp() {
		long time = 1L;
		NT nt = new NT("ABC", time);
	    assertEquals(time, nt.getTimestamp());
	}
    @Test
	public void testNT_null() {
		try {
			new NT(null, 1L);
			fail("Sucessfully called NT with a null name argument.");			
		} catch (IllegalArgumentException e) {
			// ignore -- expected
		}
	}
    @Test
	public void testEqualsObject() {
    	String name = "abc";
    	long time = 1L;
    	NT h = new NT(name, time);
    	NT h_dup = new NT(name, time);
    	NT h_dup2 = new NT(name, time);
    	NT h_diff = new NT(name + "diff", time);
    	NT h_diff2 = new NT(name, (time + 1L));
    	
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
    @Test
    public void test_hashcode() {
    	String name = "abc";
    	long time = 1L;
    	NT h = new NT(name, time);
    	NT h_dup = new NT(name, time);
    	assertTrue(h.hashCode()==h_dup.hashCode());
    }
    @Test
    public void test_toString() {
    	String name = "abc";
    	long time = 1L;
    	NT h = new NT(name, time);
    	NT h_dup = new NT(name, time);
    	String expected = "NT{name=" + "abc" + ",timestamp=" + 1L + "}";
    	assertTrue(h.toString().equals(expected));
    	assertTrue(h_dup.toString().equals(expected));
    }	

}
