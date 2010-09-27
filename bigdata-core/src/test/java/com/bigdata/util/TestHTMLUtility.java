package com.bigdata.util;

import junit.framework.TestCase;

public class TestHTMLUtility extends TestCase {

	public TestHTMLUtility(String name) {
		super(name);
	}

	public void testEscapeForXHTML() {
		String bare = "A\"'<>&z";
        String expected = "A&#34;&#39;&lt;&gt;&amp;z";		
		String actual = HTMLUtility.escapeForXHTML(bare);
		assertEquals(expected, actual);
	}
	public void testEscapeForXHTML_null() {
		try {
			HTMLUtility.escapeForXHTML(null);
			fail("Successfully called escapeForXHTML with null.");
		} catch (IllegalArgumentException e) {
			//ignore -- expected
		}
	}
	public void testEscapeForXHTML_empty() {
        String expected = "";		
		String actual = HTMLUtility.escapeForXHTML("");
		assertEquals(expected, actual);
	}

}
