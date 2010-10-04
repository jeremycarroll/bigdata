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
 * Note: These tests use the default configuration of 
 * <code>ConfigDeployUtil</code>. No attempt is made to set/get the 
 * underlying <code>Properties</code> object. Therefore, these tests
 * are going to be sensitive to the default configuration file.
 */
public class ConfigDeployUtilTest2 {


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
		
	//TODO - test empty string once available
	//TODO - test valid string once available
	//TODO - test invalid string once available
	//TODO - test string array (valid:[empty, single, multiple], invalid)
    //TODO - test int & long (valid, invalid)
    //TODO - test get boolean once available

	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getStringArray(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetString_default() throws ConfigurationException {		
		String key = "federation.name";
		String expected = "";//non-existent, empty default
		String actual = ConfigDeployUtil.getString(key);
		assertEquals(actual, expected);				
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
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getDescription(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetDescription_bogus() throws ConfigurationException {
		String key = "testGetDescription_bogus";
        String expected = null;
		String actual = ConfigDeployUtil.getDescription(key);
		assertEquals(actual, expected);		
	}
	
	/**
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getDescription(java.lang.String)}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetDescription_valid() throws ConfigurationException {
		String key = "federation.name";
        String expected = "Name of the federation to discover";
		String actual = ConfigDeployUtil.getDescription(key);
		assertEquals(actual, expected);		
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
		String key = "federation.name";
		String expected = "";
		String actual = ConfigDeployUtil.getDefault(key);
		assertEquals(actual, expected);		
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
	 * Test method for {@link com.bigdata.util.config.ConfigDeployUtil#getFederationName()}.
	 * @throws ConfigurationException 
	 */
	@Test
	public void testGetFederationName() throws ConfigurationException {
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
		
		ServiceInfo info = ConfigDeployUtil.initServiceInfo(uuid, serviceName);
        assertEquals(info.source, uuid);
        assertEquals(info.serviceName, serviceName);
        assertEquals(info.nodeName, "");
        assertEquals(info.uNumber, new Integer("-1"));
        assertEquals(info.rack, "rack-0000");
        assertEquals(info.cage, "cage-000");
        assertEquals(info.zone, "zone-00");
        assertEquals(info.site, "site-00");
        assertEquals(info.region, "region-00");
        assertEquals(info.geo, "geo-00");
	}	
	
	// TODO  test overrides file -- main() routine?
	
}
