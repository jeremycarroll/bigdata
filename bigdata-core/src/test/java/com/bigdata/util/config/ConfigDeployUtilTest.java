/**
 * 
 */
package com.bigdata.util.config;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.SocketException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import net.jini.config.ConfigurationException;
import net.jini.core.discovery.LookupLocator;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.bigdata.attr.ServiceInfo;
import com.ibm.icu.text.NumberFormat;

/**
 * Note: These tests use a package protected method 
 * {@link com.bigdata.util.config.ConfigDeployUtil#getDeploymentProperties()}
 * to set its underlying <code>Properties</code> object. 
 */
public class ConfigDeployUtilTest {
	/**
	 * Holds a reference to the default <code>Properties</code> object created by
	 * <code>ConfigDeployUtil</code>. The reference is reset on the 
	 * <code>ConfigDeployUtil</code> object after these unit tests finish.
	 */
	private static Properties _old_properties = null;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		_old_properties = ConfigDeployUtil.getDeploymentProperties();		
		ConfigDeployUtil.setDeploymentProperties(new Properties());				
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		ConfigDeployUtil.setDeploymentProperties(_old_properties);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		ConfigDeployUtil.getDeploymentProperties().clear();						
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getString(java.lang.String)}.
	 */
	@Test
	public void testGetString_bogus() {		
		try {
			String k = "testGetString_bogus";
			String v = ConfigDeployUtil.getString(k);
			fail("Successfully called getString with bogus parameter string: [" 
				+ k + ":" + v + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getString(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetString_empty_name() throws ConfigurationException {	
		String key = "testGetString_empty_name";
        String value = "";
		ConfigDeployUtil.getDeploymentProperties().setProperty(
				key, value);					
		String actual = ConfigDeployUtil.getString(key);
		String expected = value;
		assertEquals(actual, expected);
	}

	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getString(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetString_valid_name() throws ConfigurationException {	
		String key = "testGetString_valid_name";
        String value = key + "_value";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key, value);					
		p.setProperty(key+".stringvals", value + ",other");							
		String actual = ConfigDeployUtil.getString(key);
		String expected = value;
		assertEquals(actual, expected);
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getString(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetString_invalid_name() throws ConfigurationException {	
		String key = "testGetString_invalid_name";
        String value = key + "_value";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key, value);					
		p.setProperty(key+".stringvals", "other1,other2");							
		try {
			ConfigDeployUtil.getString(key);
			fail("Successfully called getString with bogus parameter string: [" 
					+ key + ":" + value + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetStringArray_array_bogus() throws ConfigurationException {		
		String key = "testGetStringArray_array_bogus";
		String value = null;
		try {
			ConfigDeployUtil.getStringArray(key);
			fail("Successfully called getStringArray with bogus parameter string: [" 
					+ key + ":" + value + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}	

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetStringArray_array_default_singleton() throws ConfigurationException {		
		String key = "testGetStringArray_array_default_singleton";
        String value = "infrastructure";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", value);					
		p.setProperty(key+".type", "string");					
		p.setProperty(key+".stringvals", value + ",non-infrastructure,other");							
		String[] actual = ConfigDeployUtil.getStringArray(key);
		String[] expected = {value};
		assertArrayEquals(actual, expected);
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetStringArray_valid_singleton() throws ConfigurationException {		
		String key = "testGetStringArray_valid_singleton";
        String value = "infrastructure";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", value + "_default");					
		p.setProperty(key, value);					
		p.setProperty(key+".type", "string");					
		p.setProperty(key+".stringvals", value + ",non-infrastructure,other");							
		String[] actual = ConfigDeployUtil.getStringArray(key);
		String[] expected = {value};
		assertArrayEquals(actual, expected);
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetStringArray_invalid_singleton() throws ConfigurationException {		
		String key = "testGetStringArray_invalid_singleton";
        String value = "infrastructure";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", value + "_default");					
		p.setProperty(key, value);					
		p.setProperty(key+".type", "string");					
		p.setProperty(key+".stringvals", "non-infrastructure,other");							
		try {
			ConfigDeployUtil.getStringArray(key);
			fail("Successfully called getStringArray with invalid parameter string: [" 
					+ key + ":" + value + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetStringArray_array_default_multiple() throws ConfigurationException {		
		String key = "testGetStringArray_array_default_multiple";
        String value = "infrastructure1,infrastructure2";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", value);					
		p.setProperty(key+".type", "string");					
		p.setProperty(key+".stringvals", value + ",non-infrastructure,other");							
		String[] actual = ConfigDeployUtil.getStringArray(key);
		String[] expected = value.split(",");
		assertArrayEquals(actual, expected);
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetStringArray_array_valid_multiple() throws ConfigurationException {		
		String key = "testGetStringArray_array_valid_multiple";
        String value = "infrastructure1,infrastructure2";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", value);					
		p.setProperty(key+".type", "string");					
		p.setProperty(key+".stringvals", value + ",non-infrastructure,other");							
		String[] actual = ConfigDeployUtil.getStringArray(key);
		String[] expected = value.split(",");
		assertArrayEquals(actual, expected);
	}	

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetStringArray_array_invalid_multiple1() throws ConfigurationException {		
		String key = "testGetStringArray_array_invalid_multiple1";
        String value = "infrastructure1,infrastructure2";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", value);					
		p.setProperty(key+".type", "string");					
		p.setProperty(key+".stringvals", "non-infrastructure,other");							
		try {
			ConfigDeployUtil.getStringArray(key);
			fail("Successfully called getStringArray with invalid parameter string: [" 
					+ key + ":" + value + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetStringArray_array_invalid_multiple2() throws ConfigurationException {		
		String key = "testGetStringArray_array_invalid_multiple2";
        String value = "infrastructure1,infrastructure2";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", value);					
		p.setProperty(key+".type", "string");					
		p.setProperty(key+".stringvals", "infrastructure1,non-infrastructure,other");							
		try {
			ConfigDeployUtil.getStringArray(key);
			fail("Successfully called getStringArray with invalid parameter string: [" 
					+ key + ":" + value + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}	

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getInt(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_bogus() throws ConfigurationException {
		String key = "testGetInt_bogus";
		String value = null;
		try {
			ConfigDeployUtil.getInt(key);
			fail("Successfully called getInt with invalid parameter string: [" 
					+ key + ":" + value + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_valid1() throws ConfigurationException {		
		String key = "testGetInt_valid1";
        String sValue = "2";
        int expected = Integer.parseInt(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "1000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		int actual = ConfigDeployUtil.getInt(key);
		assertEquals(actual, expected);
	}	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_valid2() throws ConfigurationException {		
		String key = "testGetInt_valid2";
        String sValue = "-1";
        int expected = Integer.parseInt(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "1000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		int actual = ConfigDeployUtil.getInt(key);
		assertEquals(actual, expected);
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_valid3() throws ConfigurationException {		
		String key = "testGetInt_valid3";
        String sValue = "1000";
        int expected = Integer.parseInt(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "1000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		int actual = ConfigDeployUtil.getInt(key);
		assertEquals(actual, expected);
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_valid4() throws ConfigurationException {		
		String key = "testGetInt_valid4";
        String sValue = Integer.toString(Integer.MAX_VALUE);
        int expected = Integer.MAX_VALUE;
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", sValue);					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		int actual = ConfigDeployUtil.getInt(key);
		assertEquals(actual, expected);
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_valid5() throws ConfigurationException {		
		String key = "testGetInt_valid5";
        String sValue = Integer.toString(Integer.MIN_VALUE);
        int expected = Integer.MIN_VALUE;
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", sValue);					
		p.setProperty(key+".max", "1000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		int actual = ConfigDeployUtil.getInt(key);
		assertEquals(actual, expected);
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 * @throws ParseException 
	 */
	@Test
	public void testGetInt_valid6() throws ConfigurationException, ParseException {		
		String key = "testGetInt_valid5";
        String sValue = "1,000,000";//try string with separators
        int expected = NumberFormat.getInstance().parse(sValue).intValue();
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "0");					
		p.setProperty(key+".min", "-1,000");					
		p.setProperty(key+".max", "10,000,000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		int actual = ConfigDeployUtil.getInt(key);
		assertEquals(actual, expected);
	}	

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_invaild_min() throws ConfigurationException {		
		String key = "testGetInt_invaild_min";
        String sValue = "-2";
        int expected = Integer.parseInt(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "1000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getInt(key);
			fail("Successfully called getInt with invalid parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_invaild_max() throws ConfigurationException {		
		String key = "testGetInt_invaild_max";
        String sValue = "2000";
        int expected = Integer.parseInt(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "1000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getInt(key);
			fail("Successfully called getInt with invalid parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}	

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_invaild_not_a_number() throws ConfigurationException {		
		String key = "testGetInt_invaild_not_a_number";
        String sValue = "abcxyz";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "10000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getInt(key);
			fail("Successfully called getLong with getLong parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (NumberFormatException e) {
			//ignore -- exception
		}		
	}	

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_invaild_max_not_a_number() throws ConfigurationException {		
		String key = "testGetInt_invaild_max_not_a_number";
        String sValue = "10000";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "bogus_max_value");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getInt(key);
			fail("Successfully called getLong with getLong parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (NumberFormatException e) {
			//ignore -- exception
		}		
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetInt_invaild_min_not_a_number() throws ConfigurationException {		
		String key = "testGetInt_invaild_min_not_a_number";
        String sValue = "-1";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "bogus_min_value");					
		p.setProperty(key+".max", "10000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getInt(key);
			fail("Successfully called getLong with getLong parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (NumberFormatException e) {
			//ignore -- exception
		}		
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getLong(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_bogus() throws ConfigurationException {
		String key = "testGetLong_bogus";
		String sValue = "2000";
		try {
			ConfigDeployUtil.getLong(key);
			fail("Successfully called getLong with bogus parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_valid1() throws ConfigurationException {		
		String key = "testGetLong_valid1";
        String sValue = "2222";
        long expected = Long.parseLong(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "10000");					
		p.setProperty(key+".type", "long");							
		p.setProperty(key, sValue);							
		long actual = ConfigDeployUtil.getLong(key);
		assertEquals(actual, expected);
	}		
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_valid2() throws ConfigurationException {		
		String key = "testGetLong_valid2";
        String sValue = "-1";
        long expected = Long.parseLong(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "10000");					
		p.setProperty(key+".type", "long");							
		p.setProperty(key, sValue);							
		long actual = ConfigDeployUtil.getLong(key);
		assertEquals(actual, expected);
	}		
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_valid3() throws ConfigurationException {		
		String key = "testGetLong_valid3";
        String sValue = "10000";
        long expected = Long.parseLong(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "10000");					
		p.setProperty(key+".type", "long");							
		p.setProperty(key, sValue);							
		long actual = ConfigDeployUtil.getLong(key);
		assertEquals(actual, expected);
	}		

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_valid4() throws ConfigurationException {		
		String key = "testGetLong_valid4";
        String sValue = Long.toString(Long.MAX_VALUE);
        long expected = Long.parseLong(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", sValue);					
		p.setProperty(key+".type", "long");							
		p.setProperty(key, sValue);							
		long actual = ConfigDeployUtil.getLong(key);
		assertEquals(actual, expected);
	}		

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_valid5() throws ConfigurationException {		
		String key = "testGetLong_valid5";
        String sValue = Long.toString(Long.MIN_VALUE);
        long expected = Long.parseLong(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", sValue);					
		p.setProperty(key+".max", "10000");					
		p.setProperty(key+".type", "long");							
		p.setProperty(key, sValue);							
		long actual = ConfigDeployUtil.getLong(key);
		assertEquals(actual, expected);
	}		
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 * @throws ParseException 
	 */
	@Test
	public void testGetLong_valid6() throws ConfigurationException, ParseException {		
		String key = "testGetInt_valid6";
        String sValue = "1,000,000";//try string with separators
        long expected = NumberFormat.getInstance().parse(sValue).longValue();
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "0");					
		p.setProperty(key+".min", "-1,000");					
		p.setProperty(key+".max", "10,000,000");					
		p.setProperty(key+".type", "long");							
		p.setProperty(key, sValue);							
		long actual = ConfigDeployUtil.getLong(key);
		assertEquals(actual, expected);
	}	

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_invaild_min() throws ConfigurationException {		
		String key = "testGetLong_invaild_min";
        String sValue = "-20000";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "10000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getLong(key);
			fail("Successfully called getInt with getLong parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}	

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_invaild_max() throws ConfigurationException {		
		String key = "testGetLong_invaild_max";
        String sValue = "20000";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "10000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getLong(key);
			fail("Successfully called getLong with getLong parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_invaild_not_a_number() throws ConfigurationException {		
		String key = "testGetLong_invaild_max";
        String sValue = "abcxyz";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "10000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getLong(key);
			fail("Successfully called getLong with getLong parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (NumberFormatException e) {
			//ignore -- exception
		}		
	}	
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_invaild_max_not_a_number() throws ConfigurationException {		
		String key = "testGetLong_invaild_max";
        String sValue = "1000";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "-1");					
		p.setProperty(key+".max", "bogus_max_value");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getLong(key);
			fail("Successfully called getLong with getLong parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (NumberFormatException e) {
			//ignore -- exception
		}		
	}		

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLong_invaild_min_not_a_number() throws ConfigurationException {		
		String key = "testGetLong_invaild_max";
        String sValue = "1000";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "-1");					
		p.setProperty(key+".min", "bogus_min_value");					
		p.setProperty(key+".max", "10000");					
		p.setProperty(key+".type", "int");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getLong(key);
			fail("Successfully called getLong with getLong parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (NumberFormatException e) {
			//ignore -- exception
		}		
	}		

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getBoolean(java.lang.String)}.
	 */
	@Test
	public void testGetBoolean_bogus() {
		try {
			String k = "bogus.field.name";
			boolean v = ConfigDeployUtil.getBoolean(k);
			fail("Successfully called getBoolean with bogus parameter string: [" 
				+ k + ":" + v + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getBoolean(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetBoolean_valid_true1() throws ConfigurationException {
		String key = "testGetBoolean_valid_true1";
        String sValue = "true";
        boolean expected = Boolean.parseBoolean(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "false");					
		p.setProperty(key+".type", "boolean");							
		p.setProperty(key, sValue);							
		boolean actual = ConfigDeployUtil.getBoolean(key);
		assertEquals(actual, expected);
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getBoolean(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetBoolean_valid_true2() throws ConfigurationException {
		String key = "testGetBoolean_valid_true2";
        String sValue = "TrUe";
        boolean expected = Boolean.parseBoolean(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "false");					
		p.setProperty(key+".type", "boolean");							
		p.setProperty(key, sValue);							
		boolean actual = ConfigDeployUtil.getBoolean(key);
		assertEquals(actual, expected);
	}
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getBoolean(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetBoolean_valid_false1() throws ConfigurationException {
		String key = "testGetBoolean_valid_false1";
        String sValue = "false";
        boolean expected = Boolean.parseBoolean(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "true");					
		p.setProperty(key+".type", "boolean");							
		p.setProperty(key, sValue);							
		boolean actual = ConfigDeployUtil.getBoolean(key);
		assertEquals(actual, expected);
	}
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getBoolean(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetBoolean_valid_false2() throws ConfigurationException {
		String key = "testGetBoolean_valid_false2";
        String sValue = "FaLsE";
        boolean expected = Boolean.parseBoolean(sValue);
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "true");					
		p.setProperty(key+".type", "boolean");							
		p.setProperty(key, sValue);							
		boolean actual = ConfigDeployUtil.getBoolean(key);
		assertEquals(actual, expected);
	}
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getBoolean(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetBoolean_invalid() throws ConfigurationException {
		String key = "testGetBoolean_invalid";
        String sValue = "bogus";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", "Description");					
		p.setProperty(key+".default", "true");					
		p.setProperty(key+".type", "boolean");							
		p.setProperty(key, sValue);							
		try {
			ConfigDeployUtil.getBoolean(key);
			fail("Successfully called getBoolean with invalid parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}
	}
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getDescription(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetDescription_bogus() throws ConfigurationException {
		String key = "testGetDescription_bogus";
        String expected = null;
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key, "");							
		String actual = ConfigDeployUtil.getDescription(key);
		assertEquals(actual, expected);		
	}

	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getDescription(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetDescription_valid() throws ConfigurationException {
		String key = "testGetDescription_valid";
        String expected = key + "_desc";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".description", expected);							
		String actual = ConfigDeployUtil.getDescription(key);
		assertEquals(actual, expected);		
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getType(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetType_bogus() throws ConfigurationException {
		String k = "testGetType_bogus";
		String actual = ConfigDeployUtil.getType(k);
		String expected = null;
		assertEquals(actual, expected);		
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getType(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetType_valid() throws ConfigurationException {
		String key = "testGetType_valid";
		String expected = "float";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key+".type", expected);									
		String actual = ConfigDeployUtil.getType(key);
		assertEquals(actual, expected);		
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getDefault(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetDefault_invalid() throws ConfigurationException {
		String key = "testGetDefault_invalid";
		String sValue = null;
		try {
			ConfigDeployUtil.getDefault(key);
			fail("Successfully called getDefault with bogus parameter string: [" 
					+ key + ":" + sValue + "]");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getDefault(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetDefault_valid() throws ConfigurationException {
		String key = "testGetDefault_valid";
		String expected = "defaultValue";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty(key + ".default", expected);											
		String actual = ConfigDeployUtil.getDefault(key);
		assertEquals(actual, expected);		
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getGroupsToDiscover()}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetGroupsToDiscover_bogus() throws ConfigurationException {
		try {
			ConfigDeployUtil.getGroupsToDiscover();
			fail("Successfully called getGroupsToDiscover with missing "
				+ "federation name");
		} catch (ConfigurationException e) {
			//ignore -- exception
		}		
	}
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getGroupsToDiscover()}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetGroupsToDiscover_default() throws ConfigurationException {
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty("federation.name", "");									
		String[] actual = ConfigDeployUtil.getGroupsToDiscover();		
		assertEquals(actual.length, 1);		
		assertTrue(actual[0].startsWith("bigdata.test.group-", 0));
	}
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getGroupsToDiscover()}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetGroupsToDiscover_valid_singleton() throws ConfigurationException {
		String name = "singleton";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty("federation.name", name);									
		String[] actual = ConfigDeployUtil.getGroupsToDiscover();		
		assertEquals(actual.length, 1);		
		assertEquals(actual[0], name);
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getGroupsToDiscover()}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetGroupsToDiscover_valid_multiple() throws ConfigurationException {
		String name = "m1,m2,m3";
		String[] expected = name.split(",");
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty("federation.name", name);									
		String[] actual = ConfigDeployUtil.getGroupsToDiscover();		
		assertEquals(expected.length, actual.length);		
		assertArrayEquals(actual, expected);
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getFederationName()}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetFederationName() throws ConfigurationException {
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty("federation.name", "");											
		String actual = ConfigDeployUtil.getFederationName();
		assertTrue(actual.startsWith("bigdata.test.group-", 0));
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getLocatorsToDiscover()}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetLocatorsToDiscover() throws ConfigurationException {
		LookupLocator[] locs = ConfigDeployUtil.getLocatorsToDiscover();
		assertEquals(locs.length, 0);
	}

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#initServiceInfo(java.util.UUID, java.lang.String)}.
	 * @throws ConfigurationException 
	 * @throws IOException 
	 * @throws SocketException 
	 */
	@Test
	public void testInitServiceInfo() throws SocketException, IOException, ConfigurationException {
		UUID uuid = UUID.randomUUID();
		String serviceName = "Foo";
		Properties p = ConfigDeployUtil.getDeploymentProperties();
		p.setProperty("node.name", "node.name.value");					
		p.setProperty("node.rack", "node.rack.value");					
		p.setProperty("node.cage", "node.cage.value");					
		p.setProperty("node.zone", "node.zone.value");					
		p.setProperty("node.site", "node.site.value");					
		p.setProperty("node.region", "node.region.value");					
		p.setProperty("node.geo", "node.geo.value");	
		p.setProperty("node.serviceNetwork", "default");	
		p.setProperty("node.uNumber", "123");	
		
		ServiceInfo info = ConfigDeployUtil.initServiceInfo(uuid, serviceName);
        assertEquals(info.source, uuid);
        assertEquals(info.serviceName, serviceName);
        assertEquals(info.nodeName, "node.name.value");
        assertEquals(info.uNumber, new Integer("123"));
        assertEquals(info.rack, "node.rack.value");
        assertEquals(info.cage, "node.cage.value");
        assertEquals(info.zone, "node.zone.value");
        assertEquals(info.site, "node.site.value");
        assertEquals(info.region, "node.region.value");
        assertEquals(info.geo, "node.geo.value");
	}	
	
	/*
	 *  This needs a different test class that doesn't call 
	 *  setDeploymentProperties(). It might also require dynamically
	 *  loading the ConfigDeployUtil class in order to be able to 
	 *  populate an overrides file before that class is statically initialized.
	 */
	//TODO - test default-props file
	// TODO  test overrides file -- main() routine?
	
}
