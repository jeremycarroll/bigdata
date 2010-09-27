package com.bigdata.util;

import com.bigdata.util.CSVReader.Header;

import junit.framework.TestCase;

public class TestNT extends TestCase {


	public void testGetName() {
		String name =  "ABC";
	    NT nt = new NT(name, 1L);
	    assertEquals(name, nt.getName());
	}

	public void testGetTimestamp() {
		long time = 1L;
		NT nt = new NT("ABC", time);
	    assertEquals(time, nt.getTimestamp());
	}

	public void testNT_null() {
		try {
			NT nt = new NT(null, 1L);
			fail("Sucessfully called NT with a null name argument.");			
		} catch (IllegalArgumentException e) {
			// ignore -- expected
		}
	}

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
	
    public void test_hashcode() {
    	String name = "abc";
    	long time = 1L;
    	NT h = new NT(name, time);
    	NT h_dup = new NT(name, time);
    	assertTrue(h.hashCode()==h_dup.hashCode());
    }
    
    public void test_toString() {
    	String name = "abc";
    	long time = 1L;
    	NT h = new NT(name, time);
    	NT h_dup = new NT(name, time);
    	assertTrue(h.toString().equals("NT{name=" + "abc" + ",timestamp=" + 1L + "}"));
    }	

}
