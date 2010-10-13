package com.bigdata.util.config;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class NicUtilTest {

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
	public void testGetNetworkInterface_null() throws SocketException {
		try {
			NicUtil.getNetworkInterface(null);
			fail("Successfully called getNetworkInterface() with null.");
		} catch (NullPointerException e) {
			//ignore -- expected
		}
	}

	@Test
	public void testGetNetworkInterface_by_if_name() throws SocketException {
		Enumeration<NetworkInterface> ifs = 
			NetworkInterface.getNetworkInterfaces();
		while(ifs.hasMoreElements()) {
			NetworkInterface i = ifs.nextElement();
			assertEquals(NicUtil.getNetworkInterface(i.getName()), i);
		}
	}
	
	@Test
	public void testGetNetworkInterface_by_host_name() throws SocketException {
		boolean found = false;
		for (Enumeration<NetworkInterface> ifs = 
			     NetworkInterface.getNetworkInterfaces(); 
		     ifs.hasMoreElements();) 
		{
			NetworkInterface i = ifs.nextElement();
			for (Enumeration<InetAddress> ips = i.getInetAddresses(); 
			     ips.hasMoreElements();)
			{
				InetAddress ip = ips.nextElement();
				NetworkInterface x = NicUtil.getNetworkInterface(ip.getHostAddress());
				assertNotNull(x);
				assertEquals(i, x);
				found = true;
			}	
		}

		if (!found) fail("Could not find any IP address for testing.");
	}
	
	@Test
	public void testGetNetworkInterface_by_unknown_host_name() throws SocketException {
		String unknownHost = getBogusHostname();
		NetworkInterface x = NicUtil.getNetworkInterface(unknownHost);
		assertNull(x);
	}
	
	@Test
	public void testGetNetworkInterface_by_empty_host_name() throws SocketException {
		NetworkInterface x = NicUtil.getNetworkInterface("");
		assertNotNull(x);
		assertTrue(x.isLoopback());
	}
	
	@Test
	public void testGetInetAddressMap() throws SocketException {
		Map<InetAddress, String> expected = new HashMap<InetAddress, String>();
		for (Enumeration<NetworkInterface> ifs = 
			     NetworkInterface.getNetworkInterfaces(); 
		     ifs.hasMoreElements();) 
		{
			NetworkInterface i = ifs.nextElement();
			for (Enumeration<InetAddress> ips = i.getInetAddresses(); 
			     ips.hasMoreElements();)
			{
				InetAddress ip = ips.nextElement();
				expected.put(ip, i.getName());
			}	
		}
		Map<InetAddress, String> actual = NicUtil.getInetAddressMap();
		assertEquals(actual, expected);
	}

	@Test
	public void testGetNetworkInterfaceArray_null() throws SocketException {
		try {
			NicUtil.getNetworkInterfaceArray(null);
			fail("Successfully called getNetworkInterfaceArray with null.");
		} catch (NullPointerException e) {
			// ignore -- expected
		}
	}

	@Test
	public void testGetNetworkInterfaceArray_unknown_host_name() throws SocketException {
		String unknownHost = getBogusHostname();
		NetworkInterface[] x = NicUtil.getNetworkInterfaceArray(unknownHost);
		assertEquals(x.length, 0);
	}
	
	@Test
	public void testGetNetworkInterfaceArray_known_host_name() throws SocketException {
		Enumeration<NetworkInterface> ns = NetworkInterface.getNetworkInterfaces();
		List<NetworkInterface> ls = Collections.list(ns);
		assert(ls.size()>0);
		//Pick random entry from list.
		Random r = new Random();
		//Note: size() must > 0, here.
		String ifName = ls.get(r.nextInt(ls.size())).getName();
		NetworkInterface[] nifs = NicUtil.getNetworkInterfaceArray(ifName);		
		assertTrue(nifs.length > 0);
		assertEquals(nifs[0].getName(), ifName);
	}

	@Test
	public void testGetNetworkInterfaceArray_known_ip_address() throws SocketException {
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		NetworkInterface[] nifs = NicUtil.getNetworkInterfaceArray(res.getValue1());
		assertEquals(nifs.length, 1);
		assertEquals(nifs[0], res.getValue2());		
	}

	@Test
	public void testGetNetworkInterfaceArray_all() throws SocketException {
		List<NetworkInterface> expected = 
			Collections.list(NetworkInterface.getNetworkInterfaces());
		assertTrue(expected.size() > 0);
		List<NetworkInterface> actual = 
			Arrays.asList(NicUtil.getNetworkInterfaceArray("all"));
		assertEquals(actual.size(), expected.size());
		//Remove actual from expected -- should leave an empty list
		expected.removeAll(actual);	
		assertTrue(expected.isEmpty());
	}

	@Test
	public void testGetInetAddress_npe_args() {
		String name = null;
		int index = 0;
		String host = null;
		boolean localHost = false;
		try{
			NicUtil.getInetAddress(name, index, host, localHost);
			fail("Successfully called getInetAddress with invalid arguments.");
		} catch (NullPointerException e) {
			//ignore -- expected
		}
	}
	
	@Test
	public void testGetInetAddress_negative_index() {
		String name = null;
		int index = -1;
		String host = null;
		boolean localHost = true;
		try{
			NicUtil.getInetAddress(name, index, host, localHost);
			fail("Successfully called getInetAddress with negative index.");
		} catch (IllegalArgumentException e) {
			//ignore -- expected
		}
	}
	
	@Test
	public void testGetInetAddress_get_localhost_only() throws UnknownHostException {
		String name = null;
		int index = 0;
		String host = null;
		boolean localHost = true;
		InetAddress expected = InetAddress.getLocalHost();
		InetAddress actual = NicUtil.getInetAddress(name, index, host, localHost);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetInetAddress_get_unknown_host_no_localhost() 
		throws UnknownHostException 
	{
		String name = null;
		int index = 0;
		String host = getBogusHostname();
		boolean localHost = false;
		InetAddress expected = null;
		InetAddress actual = NicUtil.getInetAddress(name, index, host, localHost);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetInetAddress_get_unknown_host_with_localhost() 
		throws UnknownHostException 
	{
		String name = null;
		int index = 0;
		String host = getBogusHostname();
		boolean localHost = true;
		InetAddress expected = InetAddress.getLocalHost();
		InetAddress actual = NicUtil.getInetAddress(name, index, host, localHost);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetInetAddress_get_known_host_no_localhost() 
		throws UnknownHostException 
	{
		String name = null;
		int index = 0;
		String host = InetAddress.getLocalHost().getHostName();
		boolean localHost = false;
		InetAddress expected = InetAddress.getLocalHost();
		InetAddress actual = NicUtil.getInetAddress(name, index, host, localHost);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetInetAddress_get_known_host_with_localhost() 
		throws UnknownHostException 
	{
		String name = null;
		int index = 0;
		String host = InetAddress.getLocalHost().getHostName();
		boolean localHost = true;
		InetAddress expected = InetAddress.getLocalHost();
		InetAddress actual = NicUtil.getInetAddress(name, index, host, localHost);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetInetAddress_get_known_name_no_host_or_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String name = res.getValue2().getName();
		int index = 0;
		String host = null;
		boolean localHost = false;
		List<InetAddress> expectedAddresses = 
			Collections.list(res.getValue2().getInetAddresses());
		assertFalse(expectedAddresses.isEmpty());
		InetAddress actual = NicUtil.getInetAddress(name, index, host, localHost);
		assertNotNull(actual);
		assertTrue(expectedAddresses.contains(actual));
	}
	
	@Test
	public void testGetInetAddress_get_known_name_with_host_no_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String name = res.getValue2().getName();
		int index = 0;
		String host = InetAddress.getLocalHost().getHostName();
		boolean localHost = false;
		List<InetAddress> expectedAddresses = 
			Collections.list(res.getValue2().getInetAddresses());
		assertFalse(expectedAddresses.isEmpty());
		InetAddress actual = NicUtil.getInetAddress(name, index, host, localHost);
		assertNotNull(actual);
		assertTrue(expectedAddresses.contains(actual));
	}

	//TODO - Another test which ensures host != result.NetworkInterface
	
	@Test
	public void testGetInetAddress_get_known_name_with_host_with_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String name = res.getValue1();
		int index = 0;
		String host = InetAddress.getLocalHost().getHostName();
		boolean localHost = true;
		List<InetAddress> expectedAddresses = 
			Collections.list(res.getValue2().getInetAddresses());
		assertFalse(expectedAddresses.isEmpty());
		InetAddress actual = NicUtil.getInetAddress(name, index, host, localHost);
		assertNotNull(actual);
		assertTrue(expectedAddresses.contains(actual));
	}
	
	//TODO - Another test which ensures host != result.NetworkInterface

	//TODO - test for multi-IP interface (e.g. index arg > 0)
	
	@Test
	public void testGetMacAddress_null() throws SocketException {
		try {
			NicUtil.getMacAddress(null);
			fail("Successfully called getMacAddress with null.");
		} catch (NullPointerException e) {
			//ignore -- exception
		}
	}
	
	@Test
	public void testGetMacAddress_unknown() throws SocketException {
		String mac = NicUtil.getMacAddress(UUID.randomUUID().toString());
		assertNull(mac);
	}
	
	@Test
	public void testGetMacAddress_known() throws SocketException, ParseException {
		Result<byte[], NetworkInterface> res =
	        getFirstNetworkInterfaceWithMacAddress();				
		String mac = NicUtil.getMacAddress(res.getValue2().getName());
		assertNotNull(mac);
		//Note: not verifying MAC address via string representation
	}
	

	@Test
	public void testGetIpAddressString_npe() {
		try {
			NicUtil.getIpAddress(null);
			fail("Successfully called getIpAddress with null.");
		} catch (NullPointerException e) {
			//ignore -- expected
		}
	}
    
	@Test
	public void testGetIpAddressString_unknown_host() {
		assertNull(NicUtil.getIpAddress(getBogusHostname()));
	}
	
	@Test
	public void testGetIpAddressString_known_host() throws SocketException {
		Result<String, NetworkInterface> res =
	        getNonLoopbackNetworkInterfaceWithInet4Address();
        String actual = 
			NicUtil.getIpAddress(res.getValue2().getName());
        assertEquals(res.getValue1(), actual);
	}
	
	@Test
	public void testGetIpAddressStringInt_npe() {
		try {
			NicUtil.getIpAddress(null, 0);
			fail("Successfully called getIpAddress with null.");
		} catch (NullPointerException e) {
			//ignore -- expected
		}
	}
	
	@Test
	public void testGetIpAddressStringInt_negative_index() {
		try {
			NicUtil.getIpAddress("", -1);
			fail("Successfully called getIpAddress with"
				+ "a negative index.");
		} catch (IllegalArgumentException e) {
			//ignore -- expected
		}
	}	
	
	@Test
	public void testGetIpAddressStringInt_unknown_host() {
		assertNull(NicUtil.getIpAddress(getBogusHostname(), 0));
	}	

	@Test
	public void testGetIpAddressStringInt_known_host() throws SocketException {
		Result<String, NetworkInterface> res =
	        getNonLoopbackNetworkInterfaceWithInet4Address();
        String actual = 
			NicUtil.getIpAddress(res.getValue2().getName(), 0);
        assertEquals(res.getValue1(), actual);
	}

	@Test
	public void testGetIpAddressStringString_npe() {
		try {
			NicUtil.getIpAddress((String)null, (String)null);
			fail("Succcessfully called getIpAddress with null.");
		}catch (NullPointerException e) {
			// ignore -- expected
		}
	}
	
	@Test
	public void testGetIpAddressStringString_unknown_name_host() {
		assertNull(NicUtil.getIpAddress((String)null, getBogusHostname()));
		assertNull(NicUtil.getIpAddress(getBogusHostname(), null));
		assertNull(NicUtil.getIpAddress(getBogusHostname(), getBogusHostname()));
	}
	
	@Test
	public void testGetIpAddressStringString_known_name_unknown_host() 
	    throws SocketException 
	{
		Result<String, NetworkInterface> result = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		String host = getBogusHostname();
		String ip = NicUtil.getIpAddress(
		    result.getValue2().getName(), host);
		assertEquals(result.getValue1(), ip);
	}
	
	@Test
	public void testGetIpAddressStringString_unknown_name_known_host() 
	    throws SocketException 
	{
		Result<String, NetworkInterface> result = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		String bogus_name = getBogusHostname();
		String ip = NicUtil.getIpAddress(
				bogus_name, result.getValue1());
		assertEquals(result.getValue1(), ip);
	}
	
	@Test
	public void testGetIpAddressStringString_known_name_host() 
	    throws SocketException 
	{
		Result<String, NetworkInterface> result = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		String name = result.getValue2().getName();
		String host = result.getValue1();
		String ip = NicUtil.getIpAddress(
				name, host);
		assertEquals(result.getValue1(), ip);
	}

	@Test
	public void testGetIpAddressStringIntString_npe() {
		try {
			NicUtil.getIpAddress((String)null, 0, (String)null);
			fail("Succcessfully called getIpAddress with null.");
		}catch (NullPointerException e) {
			// ignore -- expected
		}
	}
	
	@Test
	public void testGetIpAddressStringIntString_negative_index() {
		try {
			NicUtil.getIpAddress("", -1, "");
			fail("Successfully called getIpAddress with"
				+ "a negative index.");
		} catch (IllegalArgumentException e) {
			//ignore -- expected
		}
	}	
	@Test
	public void testGetIpAddressStringIntString_unknown_name_host() {
		assertNull(NicUtil.getIpAddress(
			(String)null, 0, getBogusHostname()));
		assertNull(NicUtil.getIpAddress(
			getBogusHostname(), 0, null));
		assertNull(NicUtil.getIpAddress(
			getBogusHostname(), 0, getBogusHostname()));
	}
	
	@Test
	public void testGetIpAddressStringIntString_known_name_unknown_host() 
	    throws SocketException 
	{
		Result<String, NetworkInterface> result = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		String host = getBogusHostname();
		String ip = NicUtil.getIpAddress(
		    result.getValue2().getName(), 0, host);
		assertEquals(result.getValue1(), ip);
	}
	@Test
	public void testGetIpAddressStringIntString_unknown_name_known_host() 
	    throws SocketException 
	{
		Result<String, NetworkInterface> result = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		String bogus_name = getBogusHostname();
		String ip = NicUtil.getIpAddress(
				bogus_name, 0, result.getValue1());
		assertEquals(result.getValue1(), ip);
	}
	
	@Test
	public void testGetIpAddressStringIntString_known_name_host() 
	    throws SocketException 
	{
		Result<String, NetworkInterface> result = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		String name = result.getValue2().getName();
		String host = result.getValue1();
		String ip = NicUtil.getIpAddress(
				name, 0, host);
		assertEquals(result.getValue1(), ip);
	}
	
	@Test
	public void testGetIpAddressStringIntStringBoolean_npe() {
		String name = null;
		int index = 0;
		String host = null;
		boolean localHost = false;
		try{
			NicUtil.getIpAddress(name, index, host, localHost);
			fail("Successfully called getIpAddress with invalid arguments.");
		} catch (NullPointerException e) {
			//ignore -- expected
		}
	}
	
	@Test
	public void testGetIpAddressStringIntStringBoolean_negative_index() {
		String name = null;
		int index = -1;
		String host = null;
		boolean localHost = true;
		try{
			NicUtil.getIpAddress(name, index, host, localHost);
			fail("Successfully called getIpAddress with negative index.");
		} catch (IllegalArgumentException e) {
			//ignore -- expected
		}
	}

	@Test
	public void testGetIpAddressStringIntStringBoolean_get_localhost_only() 
		throws UnknownHostException 
	{
		String name = null;
		int index = 0;
		String host = null;
		boolean localHost = true;
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddress(name, index, host, localHost);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressStringIntStringBoolean_get_unknown_host_no_localhost() 
		throws UnknownHostException 
	{
		String name = null;
		int index = 0;
		String host = getBogusHostname();
		boolean localHost = false;
		String expected = null;
		String actual = NicUtil.getIpAddress(name, index, host, localHost);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressStringIntStringBoolean_get_unknown_host_with_localhost() 
		throws UnknownHostException 
	{
		String name = null;
		int index = 0;
		String host = getBogusHostname();
		boolean localHost = true;
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddress(name, index, host, localHost);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetIpAddressStringIntStringBoolean_get_known_host_no_localhost() 
		throws UnknownHostException 
	{
		String name = null;
		int index = 0;
		String host = InetAddress.getLocalHost().getHostName();
		boolean localHost = false;
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddress(name, index, host, localHost);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressStringIntStringBoolean_get_known_host_with_localhost() 
		throws UnknownHostException 
	{
		String name = null;
		int index = 0;
		String host = InetAddress.getLocalHost().getHostName();
		boolean localHost = true;
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddress(name, index, host, localHost);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressStringIntStringBoolean_get_known_name_no_host_or_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String name = res.getValue2().getName();
		int index = 0;
		String host = null;
		boolean localHost = false;
		List<InetAddress> iAddresses = 
			Collections.list(res.getValue2().getInetAddresses());
		assertFalse(iAddresses.isEmpty());
		List<String> expectedAddresses = new ArrayList<String>(); 
		for (InetAddress ia: iAddresses) {
			expectedAddresses.add(ia.getHostAddress());
		}		
		String actual = NicUtil.getIpAddress(name, index, host, localHost);
		assertNotNull(actual);
		assertTrue(expectedAddresses.contains(actual));
	}
	
	@Test
	public void testGetIpAddressStringIntStringBoolean_get_known_name_with_host_no_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String name = res.getValue2().getName();
		int index = 0;
		String host = InetAddress.getLocalHost().getHostName();
		boolean localHost = false;
		List<InetAddress> iAddresses = 
			Collections.list(res.getValue2().getInetAddresses());
		assertFalse(iAddresses.isEmpty());
		List<String> expectedAddresses = new ArrayList<String>(); 
		for (InetAddress ia: iAddresses) {
			expectedAddresses.add(ia.getHostAddress());
		}		
		String actual = NicUtil.getIpAddress(name, index, host, localHost);
		assertNotNull(actual);
		assertTrue(expectedAddresses.contains(actual));
	}

	//TODO - Another test which ensures host != result.NetworkInterface
	
	@Test
	public void testGetIpAddressStringIntStringBoolean_get_known_name_with_host_with_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String name = res.getValue2().getName();
		int index = 0;
		String host = InetAddress.getLocalHost().getHostName();
		boolean localHost = true;
		List<InetAddress> iAddresses = 
			Collections.list(res.getValue2().getInetAddresses());
		assertFalse(iAddresses.isEmpty());
		List<String> expectedAddresses = new ArrayList<String>(); 
		for (InetAddress ia: iAddresses) {
			expectedAddresses.add(ia.getHostAddress());
		}		
		String actual = NicUtil.getIpAddress(name, index, host, localHost);
		assertNotNull(actual);
		assertTrue(expectedAddresses.contains(actual));
	}
	
	//TODO - Another test which ensures host != result.NetworkInterface

	//TODO - test for multi-IP interface (e.g. index arg > 0)
	
	@Test
	public void testGetIpAddressStringBoolean_null_no_localhost() {
		try{
			NicUtil.getIpAddress(null, false);
			fail("Successfully called getIpAddress with null.");
		} catch (NullPointerException e) {
			//ignore -- expected
		}
	}
	
	@Test
	public void testGetIpAddressStringBoolean_null_with_localhost() throws UnknownHostException {
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddress(null, true);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetIpAddressStringBoolean_unknown_no_localhost() throws UnknownHostException {
		String expected = null;
		String actual = NicUtil.getIpAddress(getBogusHostname(), false);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressStringBoolean_unknown_with_localhost() throws UnknownHostException {
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddress(getBogusHostname(), true);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressStringBoolean_known_no_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String expected = res.getValue1();
		String actual = 
			NicUtil.getIpAddress(res.getValue2().getName(), false);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressStringBoolean_known_with_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String expected = res.getValue1();
		String actual = 
			NicUtil.getIpAddress(res.getValue2().getName(), true);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetIpAddressStringIntBoolean_null_no_localhost() {
		try{
			NicUtil.getIpAddress(null, 0, false);
			fail("Successfully called getIpAddress with null.");
		} catch (NullPointerException e) {
			//ignore -- expected
		}
	}
	
	@Test
	public void testGetIpAddressStringIntBoolean_negative_index() {
		try{
			NicUtil.getIpAddress("", -42, false);
			fail("Successfully called getIpAddress with negative index.");
		} catch (IllegalArgumentException e) {
			//ignore -- expected
		}
	}
	
	@Test
	public void testGetIpAddressStringIntBoolean_null_with_localhost() throws UnknownHostException {
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddress(null, 0, true);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetIpAddressStringIntBoolean_unknown_no_localhost() throws UnknownHostException {
		String expected = null;
		String actual = NicUtil.getIpAddress(getBogusHostname(), 0, false);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressStringIntBoolean_unknown_with_localhost() throws UnknownHostException {
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddress(getBogusHostname(), 0, true);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressStringIntBoolean_known_no_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String expected = res.getValue1();
		String actual = 
			NicUtil.getIpAddress(res.getValue2().getName(), 0, false);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressStringIntBoolean_known_with_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String expected = res.getValue1();
		String actual = 
			NicUtil.getIpAddress(res.getValue2().getName(), 0, true);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetIpAddressByHostString_npe() {
		try {
			NicUtil.getIpAddressByHost(null);
			fail("Successfully called getIpAddressByHost with null.");
		} catch (NullPointerException e) {
			//ignore -- expected
		}
	}
    
	@Test
	public void testGetIpAddressByHostString_unknown_host() {
		assertNull(NicUtil.getIpAddressByHost(getBogusHostname()));
	}
	
	@Test
	public void testGetIpAddressByHostString_known_host() throws SocketException {
		Result<String, NetworkInterface> res =
	        getNonLoopbackNetworkInterfaceWithInet4Address();
        String actual = 
			NicUtil.getIpAddressByHost(res.getValue1());
        assertEquals(res.getValue1(), actual);
	}

	@Test
	public void testGetIpAddressByHostStringBoolean_null_no_localhost() {
		try{
			NicUtil.getIpAddressByHost(null, false);
			fail("Successfully called getIpAddressByHost with null.");
		} catch (NullPointerException e) {
			//ignore -- expected
		}
	}
	
	@Test
	public void testGetIpAddressByHostStringBoolean_null_with_localhost() 
		throws UnknownHostException 
	{
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddressByHost(null, true);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetIpAddressByHostStringBoolean_unknown_no_localhost() 
		throws UnknownHostException 
	{
		String expected = null;
		String actual = NicUtil.getIpAddressByHost(getBogusHostname(), false);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressByHostStringBoolean_unknown_with_localhost() 
		throws UnknownHostException 
	{
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddressByHost(getBogusHostname(), true);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressByHostStringBoolean_known_no_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String expected = res.getValue1();
		String actual = 
			NicUtil.getIpAddressByHost(res.getValue1(), false);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressByHostStringBoolean_known_with_localhost() 
		throws UnknownHostException, SocketException 
	{
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();		
		String expected = res.getValue1();
		String actual = 
			NicUtil.getIpAddressByHost(res.getValue1(), true);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetIpAddressByLocalHost() 
		throws UnknownHostException 
	{
		String expected = InetAddress.getLocalHost().getHostAddress();
		String actual = NicUtil.getIpAddressByLocalHost();
		assertEquals(expected, actual);
	}

	@Test
	public void testGetIpAddressStringStringBoolean_vaild_prop() 
		throws SocketException, IOException 
	{
		// Valid property should return specified NIC address
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		NetworkInterface nic = res.getValue2();
		String prop = UUID.randomUUID().toString();
		String propValue = nic.getName();
		System.setProperty(prop, propValue);	
		Object[][] args = {
				{propValue, Boolean.TRUE},
				{propValue, Boolean.FALSE},
				{getBogusHostname(), Boolean.TRUE},
				{getBogusHostname(), Boolean.FALSE},
				{"default", Boolean.FALSE},
				{"default", Boolean.TRUE},
				{"", Boolean.FALSE},
				{"", Boolean.TRUE},
				{null, Boolean.FALSE},
				{null, Boolean.TRUE},
		};
		for (Object[] arg: args) {
			String actual = 
				NicUtil.getIpAddress(
					prop, (String)arg[0], ((Boolean)arg[1]).booleanValue());
			assertTrue(validNicAddress(nic, actual));
			//Negative test for insurance
			assertFalse(validNicAddress(nic, actual+"_bogus"));
		}
	}
	
	private boolean validNicAddress(NetworkInterface nic, String ip) {
        boolean found = false;
        List<InterfaceAddress> interfaceAddrs = 
        	nic.getInterfaceAddresses();
        for(InterfaceAddress interfaceAddr : interfaceAddrs) {
        	if (ip.equals(interfaceAddr.getAddress().getHostAddress())) {
        		found = true;
        		break;
        	}            	
        }	
        return found;
	}

	@Test
	public void testGetIpAddressStringStringBoolean_invaild_prop_valid_default_false() 
		throws SocketException, IOException 
	{
		//Property set but no associated nic and loopback false --> return null
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		String propName = UUID.randomUUID().toString();
		System.setProperty(propName, propName);			
		String validDefault = res.getValue2().getName();
		String invalidDefault = validDefault + "_bogus";
        Object[][] args = {
				{validDefault, Boolean.FALSE},
				{invalidDefault, Boolean.FALSE},
		};
		for (Object[] arg: args) {
			String actual = 
				NicUtil.getIpAddress(
						propName, (String)arg[0], ((Boolean)arg[1]).booleanValue());
			assertNull(actual);
		}
	}	
	
	@Test
	public void testGetIpAddressStringStringBoolean_invaild_prop_valid_default_true() 
		throws SocketException, IOException 
	{
		//Property set but no associated nic and loopback true --> return loopback
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		String propName = UUID.randomUUID().toString();
		System.setProperty(propName, propName);			
		String validDefault = res.getValue2().getName();
		String invalidDefault = validDefault + "_bogus";
        Object[][] args = {
				{validDefault, Boolean.TRUE},
				{invalidDefault, Boolean.TRUE},
		};
		for (Object[] arg: args) {
			String ip = 
				NicUtil.getIpAddress(
					propName, (String)arg[0], ((Boolean)arg[1]).booleanValue());
			assertTrue(InetAddress.getByName(ip).isLoopbackAddress());		
		}
	}	

	@Test
	public void testGetIpAddressStringStringBoolean_unset_prop_valid_default() 
		throws SocketException, IOException 
	{
		//Unset property, valid default --> return default nic IP
		Result<String, NetworkInterface> res = 
			getNonLoopbackNetworkInterfaceWithInet4Address();
		String propName = UUID.randomUUID().toString();
		String validDefault = res.getValue2().getName();
		// Note: Property not set, valid default
        Object[][] args = {
				{validDefault, Boolean.TRUE},
				{validDefault, Boolean.FALSE},
		};
		for (Object[] arg: args) {
			String actual = 
				NicUtil.getIpAddress(
					propName, (String)arg[0], ((Boolean)arg[1]).booleanValue());
			assertTrue(validNicAddress(res.getValue2(), actual));
		}
		for (Object[] arg: args) {
			String actual = 
				NicUtil.getIpAddress(
					null, (String)arg[0], ((Boolean)arg[1]).booleanValue());
			assertTrue(validNicAddress(res.getValue2(), actual));
		}
	}		
	
	@Test
	public void testGetIpAddressStringStringBoolean_unset_prop_invalid_default_true() 
		throws SocketException, IOException 
	{
		//Unset property, invalid default, loopback true --> return loopback
		String propName = UUID.randomUUID().toString();
		String invalidDefault = propName;
		String actual = NicUtil.getIpAddress(propName, invalidDefault, true);
		assertTrue(InetAddress.getByName(actual).isLoopbackAddress());		
	}		
	
	@Test
	public void testGetIpAddressStringStringBoolean_unset_prop_invalid_default_false() 
		throws SocketException, IOException 
	{
		//Unset property, invalid default, loopback false --> return null
		String propName = UUID.randomUUID().toString();
		String invalidDefault = propName;
		String actual = NicUtil.getIpAddress(propName, invalidDefault, false);
		assertNull(actual);
	}		

	@Test
	public void testGetIpAddressStringStringBoolean_unset_prop_default() 
		throws SocketException, IOException 
	{
		//Unset property,  default --> return any valid IP
		String propName = UUID.randomUUID().toString();
		String validDefault = "default";
		// Note: Property not set, valid default
        Boolean[][] args = {
				{Boolean.TRUE},
				{Boolean.FALSE},
		};
		for (Boolean[] arg: args) {
			String actual = 
				NicUtil.getIpAddress(
					propName, validDefault, arg[0].booleanValue());
			assertTrue(ipv4Exists(actual, true) // valid loopback IP, or
				|| ipv4Exists(actual, false));  // valid non-loopback IP?
		}
	}		

	@Test
	public void testGetIpAddressStringStringBoolean_unset_prop_empty() 
		throws SocketException, IOException 
	{
		//Unset property,  empty --> return any valid IP
		String propName = UUID.randomUUID().toString();
		String validDefault = ""; //empty string for nic should return loopback
        Boolean[][] args = {
				{Boolean.TRUE},
				{Boolean.FALSE},
		};
		for (Boolean[] arg: args) {
			String actual = 
				NicUtil.getIpAddress(
					propName, validDefault, arg[0].booleanValue());
			assertTrue(ipv4Exists(actual, true) // valid loopback IP, or
				|| ipv4Exists(actual, false));  // valid non-loopback IP?
		}
	}		

	@Test
	public void testGetIpAddressStringStringBoolean_unset_prop_null() 
		throws SocketException, IOException 
	{
		//Unset property,  null, true --> null
		String propName = UUID.randomUUID().toString();
		String defNic = null; 
        Boolean[][] args = {
				{Boolean.TRUE},
				{Boolean.FALSE},
		};
		for (Boolean[] arg: args) {
			String actual = 
				NicUtil.getIpAddress(
					propName, defNic, arg[0].booleanValue());
			assertNull(actual);  
		}
	}		

	@Test
	public void testGetIpAddressStringStringBoolean_null_prop_null_default_true() 
		throws SocketException, IOException 
	{
		//Null property, null default --> return loopback
		String propName = null;
		String invalidDefault = null;
		String actual = NicUtil.getIpAddress(propName, invalidDefault, true);
		assertTrue(InetAddress.getByName(actual).isLoopbackAddress());		
	}
	
	@Test
	public void testGetIpAddressStringStringBoolean_null_prop_null_default_false() 
		throws SocketException, IOException 
	{
		//Null property, null default --> return null
		String propName = null;
		String invalidDefault = null;
		try {
			NicUtil.getIpAddress(propName, invalidDefault, false);
			fail("Successfully called getIpAddress(null, null, false).");
		} catch (NullPointerException e) {
			// ignore -- expected
		}
	}		

	@Test
	public void testGetIpAddressStringStringBoolean_null_prop_default_true() 
		throws SocketException, IOException 
	{
		//Null property, default, true --> return any IP
		String propName = null;
		String defNic = "default";
		String actual = NicUtil.getIpAddress(propName, defNic, true);
		assertTrue(ipv4Exists(actual, true) || ipv4Exists(actual, false));
	}		

	@Test
	public void testGetIpAddressStringStringBoolean_null_prop_default_false() 
		throws SocketException, IOException 
	{
		//Null property, default, false --> return non-loopback IP
		String propName = null;
		String defNic = "default";
		String actual = NicUtil.getIpAddress(propName, defNic, false);
		assertTrue(ipv4Exists(actual, false));
	}		
		
	@Test
	public void testGetDefaultIpv4AddressBoolean_false() throws IOException {
		boolean loopbackOk = false;
		String ip = NicUtil.getDefaultIpv4Address(loopbackOk);
		assertNotNull(ip);
		assertTrue(ipv4Exists(ip, loopbackOk));
		//Negative test for insurance
		assertFalse(ipv4Exists(ip+"_bogus", loopbackOk));
	}
	
	@Test
	public void testGetDefaultIpv4AddressBoolean_true() throws IOException {
		boolean loopbackOk = true;
		String ip = NicUtil.getDefaultIpv4Address(loopbackOk);
		assertNotNull(ip);
		assertTrue(ipv4Exists(ip, loopbackOk));
		//Negative test for insurance
		assertFalse(ipv4Exists(ip+"_bogus", loopbackOk));
	}
	
	
	private boolean ipv4Exists(String ip, boolean loopbackOk) throws SocketException {
        boolean found = false;
        Enumeration<NetworkInterface> nics = 
            NetworkInterface.getNetworkInterfaces();
        if (nics==null) return false;
        while( nics.hasMoreElements() && !found) {
            NetworkInterface curNic = nics.nextElement();
            List<InterfaceAddress> interfaceAddrs = 
                                       curNic.getInterfaceAddresses();
            for(InterfaceAddress interfaceAddr : interfaceAddrs) {
            	InetAddress ia = interfaceAddr.getAddress();
            	if ((!loopbackOk && ia.isLoopbackAddress()) ||
            	    (!(ia instanceof Inet4Address))){
            		continue; //skipping loopback and non-IPV4 addresses
            	}
            	if (ip.equals(interfaceAddr.getAddress().getHostAddress())) {
            		found = true;
            		break;
            	}            	
            }	
        }
        return found;
    }

	
	@Test
	public void testGetDefaultIpv4Address() throws SocketException, IOException {
		String ip = NicUtil.getDefaultIpv4Address();
		assertNotNull(ip);
		boolean loopbackOk = false;
		assertTrue(ipv4Exists(ip, loopbackOk));
		//Negative test for insurance
		assertFalse(ipv4Exists(ip+"_bogus", loopbackOk));
	}

	/**
	 * Utility method that tries to locate a non-loopback
	 * <code>NetworkInterface</code> with an associated IPv4
	 * address (parallel to NicUtil).
	 * @return Result<String, NetworkInterface> containing the
	 *     the <code>String</code> version of the IPv4 address
	 *     and the associated <code>NetworkInterface</code>.
	 * @throws SocketException if there was a problem locating
	 *     a network interface with an associated IPv4 address.
	 */
	private static Result<String, NetworkInterface> 
	    getNonLoopbackNetworkInterfaceWithInet4Address() 
	    	throws SocketException 
	{
		List<NetworkInterface> networkInterfaces = 
			Collections.list(NetworkInterface.getNetworkInterfaces());
		assertTrue(networkInterfaces.size()>0);
		// Search for network interface with an associated IPv4
		String known_ip = null;
		NetworkInterface known_nif = null;
		for (NetworkInterface networkInterface: networkInterfaces) {
			List<InterfaceAddress> ifas = 
				networkInterface.getInterfaceAddresses();
			for (InterfaceAddress ifa: ifas) {
				InetAddress ia = ifa.getAddress();
				if (ia instanceof Inet4Address &&
					(!ia.isLoopbackAddress())) {
					known_ip = ia.getHostAddress();
					known_nif = networkInterface;
					break;
				}
			}
		}
		if (known_nif==null) throw new SocketException("No available interface.");
		return new Result<String, NetworkInterface>(known_ip, known_nif);		
	}
	
	private static Result<byte[], NetworkInterface> 
	    getFirstNetworkInterfaceWithMacAddress() 
	    	throws SocketException 
	{
		List<NetworkInterface> networkInterfaces = 
			Collections.list(NetworkInterface.getNetworkInterfaces());
		assertTrue(networkInterfaces.size()>0);
		// Search for network interface with an associated IP
		byte[] known_mac = null;
		NetworkInterface known_nif = null;
		for (NetworkInterface networkInterface: networkInterfaces) {
			byte[] hwAddr = networkInterface.getHardwareAddress();
			if ((hwAddr != null) && (hwAddr.length > 0)) {
				known_mac = hwAddr;
				known_nif = networkInterface;
				break;
			}
		}
		if (known_nif==null) throw new SocketException("No available interface.");
		return new Result<byte[], NetworkInterface>(known_mac, known_nif);		
	}
	
	private static class Result<X, Y> {
		private final X value1;
		private final Y value2;
		public Result(X x, Y y) {
			value1 = x;
			value2 = y;
		}
		public X getValue1() {
			return value1;
		}
		public Y getValue2() {
			return value2;
		}
	}
	private static String getBogusHostname() {
		String n = UUID.randomUUID().toString() + "_bogus_name";
		return n;
	}
}
