package com.bigdata.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestHTMLUtility {

	public TestHTMLUtility() {
		super();
	}
	
    @Test
	public void testEscapeForXHTML() {
		String bare = "A\"'<>&z";
        String expected = "A&#34;&#39;&lt;&gt;&amp;z";		
		String actual = HTMLUtility.escapeForXHTML(bare);
		assertEquals(expected, actual);
	}
    @Test
	public void testEscapeForXHTML_null() {
		try {
			HTMLUtility.escapeForXHTML(null);
			fail("Successfully called escapeForXHTML with null.");
		} catch (IllegalArgumentException e) {
			//ignore -- expected
		}
	}
    @Test
	public void testEscapeForXHTML_empty() {
        String expected = "";		
		String actual = HTMLUtility.escapeForXHTML("");
		assertEquals(expected, actual);
	}

}
