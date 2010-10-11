package com.bigdata.util.config;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConfigurationUtilTest {

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
	public void testConcat_null() throws SecurityException, NoSuchMethodException {
        String[][][] args = {
        		{null, null},
        		{new String[0], null},
        		{null, new String[0]}
        };
        for (String[][] arg: args) {
        	try {
        		ConfigurationUtil.concat(arg[0], arg[1]);
        		fail("Successfully called concat() with null arg.");
        	} catch (NullPointerException e) {
        		// ignore -- expected.
        	}
        }
	}
	
	@Test
	public void testConcat_empty() throws SecurityException, NoSuchMethodException {		
        String [] actual = ConfigurationUtil.concat(new String[0], new String[0]);
        assertTrue(actual.length==0);
	}

	@Test
	public void testConcat_non_empty_singleton() throws SecurityException, NoSuchMethodException {
		String[] a = { "abc" };
		String[] b = { "xyz" };
        String [] actual = ConfigurationUtil.concat(a, b);
        assertTrue(actual.length==(a.length + b.length));
        assertArrayEquals("Arrays don't match", actual, combine(a, b));
	}

	@Test
	public void testConcat_non_empty_multiple() throws SecurityException, NoSuchMethodException {
		String[] a = { "abc", "def", "ghi", "jklmnop"  };
		String[] b = { "qrs", "tuv", "wxyz" };
        String [] actual = ConfigurationUtil.concat(a, b);
        assertTrue(actual.length==(a.length + b.length));
        List<String> l = new ArrayList<String>();
        l.addAll(Arrays.asList(a));
        l.addAll(Arrays.asList(b));
        assertArrayEquals("Arrays don't match", actual, l.toArray(new String[0]));
	}

	@Test
	public void testConcat_non_empty_multiple_embedded_null() 
	    throws SecurityException, NoSuchMethodException 
	{
		String[] a = { "abc", "def", null, "jklmnop"  };
		String[] b = { "qrs", null, "wxyz" };
        String [] actual = ConfigurationUtil.concat(a, b);
        assertTrue(actual.length==(a.length + b.length));
        List<String> l = new ArrayList<String>();
        l.addAll(Arrays.asList(a));
        l.addAll(Arrays.asList(b));
        assertArrayEquals("Arrays don't match", actual, l.toArray(new String[0]));
	}

	@Test
	public void testCreateArgList_null() {
		Object[][] args = {
				{null, null},
				{new String[0], null},
		};
        for (Object[] arg: args) {
        	try {
        		ConfigurationUtil.createArgList((String[])arg[0], (String)arg[1]);
        		fail("Successfully called createArgList() with null arg: " 
        				+ Arrays.asList(arg));
        	} catch (NullPointerException e) {
        		// ignore -- expected.
        	}
        }
	}
	
	@Test
	public void testCreateArgList_null2() {
		//if second arg is empty or whitespace, return first arg
		assertNull(ConfigurationUtil.createArgList(null, ""));
	}
	
	@Test
	public void testCreateArgList_null3() {
		//if second arg is empty or whitespace, return first arg
		assertNull(ConfigurationUtil.createArgList(null, " \t \t\t  "));
	}
	
	@Test
	public void testCreateArgList_empty() {
		String[] orig = {"abc"};
		String extra = "";
		String[] sum = ConfigurationUtil.createArgList(orig, extra);
		assertTrue(orig==sum);//identity obviates equals test		
	}
	
	@Test
	public void testCreateArgList_single() {
		String[] orig = {"abc"};
		String[] extraArray = new String[] {"xyz"};
		List<String> l = Arrays.asList(extraArray);
		String extra = join(l);
		String[] sum = ConfigurationUtil.createArgList(orig, extra);
		assertArrayEquals(sum, combine(orig, extraArray));
	}
	
	@Test
	public void testCreateArgList_multiple() {
		String[] orig = {"abc"};
		String[] extraArray = new String[] {"rst", "uvw", "xyz"};
		List<String> l = Arrays.asList(extraArray);
		String extra = join(l);
		String[] sum = ConfigurationUtil.createArgList(orig, extra);
		assertArrayEquals(sum, combine(orig, extraArray));
	}
	
	@Test
	public void testComputeCodebase_5_arg_null() throws IOException {
		Integer port = new Integer(5);
		String jarf = "bogus_jar_file.jar";
		String name = "bogus_name";
        Object[][] args = {
        		{null, null, port, null, "sha"},
        		{null, jarf, port, null, "sha"},
        		{name, null, port, null, "sha"},
        		{name, jarf, port, null, "sha"},
        };	
        //Test int port method
        for (Object[] arg: args) {
        	try {
        		ConfigurationUtil.computeCodebase(
        			(String)arg[0], (String)arg[1], 
        			((Integer)arg[2]).intValue(), 
        			(String)arg[3], (String)arg[4]);
        		fail("Successfully called computeCodebase() with null arg: " 
        				+ Arrays.asList(arg));
        	} catch (NullPointerException e) {
        		// ignore -- expected.
        	}       	
        }
        //Test String port method
        for (Object[] arg: args) {
        	try {
        		ConfigurationUtil.computeCodebase(
        			(String)arg[0], (String)arg[1], 
        			((Integer)arg[2]).toString(), 
        			(String)arg[3], (String)arg[4]);
        		fail("Successfully called computeCodebase() with null arg: " 
        				+ Arrays.asList(arg));
        	} catch (NullPointerException e) {
        		// ignore -- expected.
        	}       	
        }
        
    }
	
	@Test
	public void testComputeCodebase_5_arg_invalid_port() throws IOException {
		Integer port = new Integer(-1);
		//Try int port
        try {
    		ConfigurationUtil.computeCodebase(
    			"bogus_name", "bogus_jar_file.jar", 
    			port.intValue(), "bogus_src", "bogus_md");
    		fail("Successfully called computeCodebase() with negative port: " 
    				+ port);
    	} catch (IllegalArgumentException e) {
    		// ignore -- expected.
    	}       	
    	//Try String port
        try {
    		ConfigurationUtil.computeCodebase(
    			"bogus_name", "bogus_jar_file.jar", 
    			port.toString(), "bogus_src", "bogus_md");
    		fail("Successfully called computeCodebase() with negative port: " 
    				+ port);
    	} catch (IllegalArgumentException e) {
    		// ignore -- expected.
    	}       	    	
    }
	
	@Test
	public void testComputeCodebase_5_arg_valid_hostname_http() throws IOException {
		Integer port = new Integer(1234);
		String hostname = InetAddress.getLocalHost().getHostAddress();
		String jarFile = "file.jar";
		Object[][] args = {
				{hostname, jarFile, port, "src", null},
				{hostname, jarFile, port, "src", ""},
				{hostname, jarFile, port, "src", "off"},
				{hostname, jarFile, port, "src", "none"},
		};
		
		//Test int port 
		for (Object[] arg: args) {
	        String urlString = ConfigurationUtil.computeCodebase(
	        		(String)arg[0], (String)arg[1], 
	        		((Integer)arg[2]).intValue(), 
	        		(String)arg[3], (String)arg[4]); //gen http url	
	        testComputeCodebase_5_arg_valid_hostname_http_assert(
	        		urlString, hostname, port, jarFile);
		}
		
		//Test String port
		for (Object[] arg: args) {
	        String urlString = ConfigurationUtil.computeCodebase(
	        		(String)arg[0], (String)arg[1], 
	        		(String)arg[2].toString(), 
	        		(String)arg[3], (String)arg[4]); //gen http url	
	        testComputeCodebase_5_arg_valid_hostname_http_assert(
	        		urlString, hostname, port, jarFile);
		}		
    }
	
	private void testComputeCodebase_5_arg_valid_hostname_http_assert(
		String urlString, String hostname, Integer port, String jarFile) 
	    throws MalformedURLException 
	{
        URL url = new URL(urlString);
        assertEquals(url.getProtocol(), "http");
        assertEquals(url.getHost(), hostname); //use IP to avoid DNS conversion
        assertEquals(url.getPort(), port.intValue());
        assertEquals(url.getFile(), "/" + jarFile);		
	}
	
	@Test
	public void testComputeCodebase_5_arg_invalid_hostname_http() throws IOException {
		Integer port = new Integer(1234);
		String hostname = UUID.randomUUID().toString();
		String jarFile = "file.jar";
		
		//Test int port
		try {
	        ConfigurationUtil.computeCodebase(
	    	    hostname, jarFile, port.intValue(), "", "");
    		fail("Successfully called computeCodebase() with invalid hostname: " 
    				+ hostname);
	        
		} catch (UnknownHostException e) {
			//ignore -- expected
		}
		
		//Test String port
		try {
	        ConfigurationUtil.computeCodebase(
	    	    hostname, jarFile, port.toString(), "", "");
    		fail("Successfully called computeCodebase() with invalid hostname: " 
    				+ hostname);
	        
		} catch (UnknownHostException e) {
			//ignore -- expected
		}
    }	
	
	@Test
	/**
	 * Note: this test assumes a well-known jar file under Sun's JDK and as 
	 * such is JDK specific.
	 */
	public void testComputeCodebase_5_arg_valid_hostname_httpmd_md5() 
	    throws IOException, URISyntaxException 
	{
		Integer port = new Integer(1234);
		String hostname = InetAddress.getLocalHost().getHostAddress();
		String jarFile = "rt.jar";
		String jarSrcPath = System.getProperty("java.home")
		    + File.separator + "lib";
		//Test int port
        String urlString = ConfigurationUtil.computeCodebase(
        		hostname, jarFile, port.intValue(), jarSrcPath, "md5"); //gen httpmd url	
        testComputeCodebase_5_arg_valid_hostname_httpmd_md5_assert(
        		urlString, hostname, port, jarFile);             
		//Test String port
        urlString = ConfigurationUtil.computeCodebase(
        		hostname, jarFile, port.toString(), jarSrcPath, "md5"); //gen httpmd url	
        testComputeCodebase_5_arg_valid_hostname_httpmd_md5_assert(
        		urlString, hostname, port, jarFile);             
    }
	
	private void testComputeCodebase_5_arg_valid_hostname_httpmd_md5_assert( 
		String urlString, String hostname, Integer port, String jarFile) 
	    throws MalformedURLException, URISyntaxException 
	{
        URI uri = new URI(urlString);
        assertEquals(uri.getScheme(), "httpmd");
        assertEquals(uri.getHost(), hostname); //use IP to avoid DNS conversion
        assertEquals(uri.getPort(), port.intValue());
        assertTrue(uri.getPath().startsWith("/" + jarFile + ";md5="));	
	}
	 	
	@Test
	/**
	 * Note: this test assumes a well-known jar file under Sun's JDK and as 
	 * such is JDK specific.
	 */	
	public void testComputeCodebase_5_arg_valid_hostname_httpmd_sha() 
	    throws IOException, URISyntaxException 
	{
		Integer port = new Integer(1234);
		String hostname = InetAddress.getLocalHost().getHostAddress();
		String jarFile = "rt.jar";
		String jarSrcPath = System.getProperty("java.home")
		    + File.separator + "lib";
		//Test int port
        String urlString = ConfigurationUtil.computeCodebase(
        		hostname, jarFile, port.intValue(), jarSrcPath, "sha"); //gen httpmd url	
        testComputeCodebase_5_arg_valid_hostname_httpmd_sha_assert( 
    			urlString, hostname, port, jarFile);
        //Test String port
        urlString = ConfigurationUtil.computeCodebase(
        		hostname, jarFile, port.toString(), jarSrcPath, "sha"); //gen httpmd url	
        testComputeCodebase_5_arg_valid_hostname_httpmd_sha_assert( 
    			urlString, hostname, port, jarFile);
    }	

	private void testComputeCodebase_5_arg_valid_hostname_httpmd_sha_assert( 
			String urlString, String hostname, Integer port, String jarFile) 
		    throws MalformedURLException, URISyntaxException 
	{
        URI uri = new URI(urlString);
        assertEquals(uri.getScheme(), "httpmd");
        assertEquals(uri.getHost(), hostname); //use IP to avoid DNS conversion
        assertEquals(uri.getPort(), port.intValue());
        assertTrue(uri.getPath().startsWith("/" + jarFile + ";sha="));
	}

	@Test
	public void testComputeCodebase_3_arg_null() throws IOException {
		Integer port = new Integer(5);
		String jarf = "bogus_jar_file.jar";
		String name = "bogus_name";
        Object[][] args = {
        		{null, null, port},
        		{null, jarf, port},
        		{name, null, port},
        };	
        //Test int port method
        for (Object[] arg: args) {
        	try {
        		ConfigurationUtil.computeCodebase(
        			(String)arg[0], (String)arg[1], 
        			((Integer)arg[2]).intValue());
        		fail("Successfully called computeCodebase() with null arg: " 
        				+ Arrays.asList(arg));
        	} catch (NullPointerException e) {
        		// ignore -- expected.
        	}       	
        }
        //Test String port method
        for (Object[] arg: args) {
        	try {
        		ConfigurationUtil.computeCodebase(
        			(String)arg[0], (String)arg[1], 
        			((Integer)arg[2]).toString());
        		fail("Successfully called computeCodebase() with null arg: " 
        				+ Arrays.asList(arg));
        	} catch (NullPointerException e) {
        		// ignore -- expected.
        	}       	
        }        
    }
	
	@Test
	public void testComputeCodebase_3_arg_invalid_port() throws IOException {
		Integer port = new Integer(-1);
		//Try int port
        try {
    		ConfigurationUtil.computeCodebase(
    			"bogus_name", "bogus_jar_file.jar", port.intValue());
    		fail("Successfully called computeCodebase() with negative port: " 
    				+ port);
    	} catch (IllegalArgumentException e) {
    		// ignore -- expected.
    	}       	
    	//Try String port
        try {
    		ConfigurationUtil.computeCodebase(
    			"bogus_name", "bogus_jar_file.jar", port.toString());
    		fail("Successfully called computeCodebase() with negative port: " 
    				+ port);
    	} catch (IllegalArgumentException e) {
    		// ignore -- expected.
    	}       	    	
    }
	
	@Test
	public void testComputeCodebase_3_arg_valid_hostname_http() throws IOException {
		Integer port = new Integer(1234);
		String hostname = InetAddress.getLocalHost().getHostAddress();
		String jarFile = "file.jar";

        String urlString = ConfigurationUtil.computeCodebase(
            hostname, jarFile, port.intValue()); //gen http url	
        testComputeCodebase_3_arg_valid_hostname_http_assert(
        		urlString, hostname, port, jarFile);


        urlString = ConfigurationUtil.computeCodebase(
                hostname, jarFile, port.toString()); //gen http url	
        testComputeCodebase_3_arg_valid_hostname_http_assert(
        		urlString, hostname, port, jarFile);
    }
		
	private void testComputeCodebase_3_arg_valid_hostname_http_assert(
			String urlString, String hostname, Integer port, String jarFile) 
		    throws MalformedURLException 
	{
        URL url = new URL(urlString);
        assertEquals(url.getProtocol(), "http");
        assertEquals(url.getHost(), hostname); //use IP to avoid DNS conversion
        assertEquals(url.getPort(), port.intValue());
        assertEquals(url.getFile(), "/" + jarFile);		
	}
	
	@Test
	public void testComputeCodebase_3_arg_invalid_hostname_http() throws IOException {
		Integer port = new Integer(1234);
		String hostname = UUID.randomUUID().toString();
		String jarFile = "file.jar";
		
		//Test int port
		try {
	        ConfigurationUtil.computeCodebase(
	    	    hostname, jarFile, port.intValue());
    		fail("Successfully called computeCodebase() with invalid hostname: " 
    				+ hostname);	        
		} catch (UnknownHostException e) {
			//ignore -- expected
		}
		
		//Test String port
		try {
	        ConfigurationUtil.computeCodebase(
	    	    hostname, jarFile, port.toString());
    		fail("Successfully called computeCodebase() with invalid hostname: " 
    				+ hostname);	        
		} catch (UnknownHostException e) {
			//ignore -- expected
		}
    }		
	
	@Test
	/**
	 * Note: this test assumes a well-known jar file under Sun's JDK and as 
	 * such is JDK specific.
	 */
	public void testComputeCodebase_port_not_number() 
	    throws IOException, URISyntaxException 
	{
		String port = "cafebabe";
		String hostname = InetAddress.getLocalHost().getHostAddress();
		String jarFile = "rt.jar";
		String jarSrcPath = System.getProperty("java.home")
		    + File.separator + "lib";
		// 5 arg method
        try {
        	ConfigurationUtil.computeCodebase(
        		hostname, jarFile, port, jarSrcPath, "md5"); //gen httpmd url
    		fail("Successfully called computeCodebase() with invalid port: " 
    				+ port);	        
        } catch (NumberFormatException e) {
        	//ignore -- expected
        }
        // 3 arg method
        try {
        	ConfigurationUtil.computeCodebase(
        		hostname, jarFile, port); //gen httpmd url
    		fail("Successfully called computeCodebase() with invalid port: " 
    				+ port);	        
        } catch (NumberFormatException e) {
        	//ignore -- expected
        }        
    }
	
    /**
     * Helper method that joins the given collection of <code>String</code> using the
     * ASCII record separator (RS) character(\036).
     * @param s the collection of strings to join
     * @return String containing the collection's elements separated by 
     * the RS delimiter.
     */
    private static String join(Collection<String> s) {
        if (s == null || s.isEmpty()) return "";
        Iterator<String> iter = s.iterator();
        StringBuilder builder = new StringBuilder(iter.next());
        while( iter.hasNext() )
        {
            builder.append('\036').append(iter.next());
        }
        return builder.toString();
    }
    
    /**
     * Helper method that combines two <code>String</code> arrays in order in which
     * they are provided.
     * @param s the collection of strings to join
     * @return String containing the collection's elements separated by 
     * the RS delimiter.
     */
    private static String[] combine(String[]a, String[]b) {
	    List<String> l = new ArrayList<String>();
	    l.addAll(Arrays.asList(a));
	    l.addAll(Arrays.asList(b));
	    return l.toArray(new String[0]);
    }
}
