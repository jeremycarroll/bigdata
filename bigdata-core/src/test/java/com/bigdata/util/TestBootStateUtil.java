package com.bigdata.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StreamCorruptedException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationFile;
import net.jini.config.EmptyConfiguration;
import net.jini.config.ConfigurationException;
import net.jini.config.NoSuchEntryException;
import net.jini.core.lookup.ServiceID;

import org.apache.log4j.Logger;

import junit.framework.TestCase;
import junit.framework.TestCase2;

public class TestBootStateUtil extends TestCase2 {

	public TestBootStateUtil(String name) {
		super(name);
	}
	   
	public void testBootStateUtilNullConsArgs() throws SecurityException, NoSuchMethodException {
		Configuration dummyConfiguration = EmptyConfiguration.INSTANCE;
		String dummyString = "dummy";
		Class<? extends TestBootStateUtil> dummyClass = this.getClass();
		Logger dummyLogger = Logger.getLogger(dummyClass);
		
		// Command lines with a null in first three args are invalid
		Object [][] badCommandLines = {
				{null, null, null, null},
				{null, null, null, dummyLogger},
				{null, null, dummyClass, null},
				{null, null, dummyClass, dummyLogger},
				{null, dummyString, null, null},
				{null, dummyString, null, dummyLogger},
				{null, dummyString, dummyClass, null},
				{null, dummyString, dummyClass, dummyLogger},
				{dummyConfiguration, null, null, null},
				{dummyConfiguration, null, null, dummyLogger},
				{dummyConfiguration, null, dummyClass, null},
				{dummyConfiguration, null, dummyClass, dummyLogger},
				{dummyConfiguration, dummyString, null, null},
				{dummyConfiguration, dummyString, null, dummyLogger},		
		};
		
		Constructor cons = 
			BootStateUtil.class.getConstructor(Configuration.class,
				String.class, Class.class, Logger.class);
		for (int i=0; i < badCommandLines.length; i++) {
			try {
				cons.newInstance(badCommandLines[i]);
				fail("Successfully called constructor with null arg: "
					+ Arrays.asList(badCommandLines[i]));
			} catch (IllegalArgumentException e) {
				fail("unexpected exception: " + e.toString());
			} catch (InstantiationException e) {
				fail("unexpected exception: " + e.toString());
			} catch (IllegalAccessException e) {
				fail("unexpected exception: " + e.toString());
			} catch (InvocationTargetException e) {
				if (! (e.getCause() instanceof NullPointerException)){
				   fail("unexpected exception: " + e.getCause().toString());
				}
				//Otherwise ignore -- expected
			} 
		}
	}
	
	public void testBootStateUtilGoodConsArgs() throws SecurityException, NoSuchMethodException {
		Configuration dummyConfiguration = EmptyConfiguration.INSTANCE;
		String dummyString = "dummy";
		Class<? extends TestBootStateUtil> dummyClass = this.getClass();
		Logger dummyLogger = Logger.getLogger(dummyClass);
		
		// Command lines with a non-null in first three args are valid
		Object [][] goodCommandLines = {
				{dummyConfiguration, dummyString, dummyClass, null},
				{dummyConfiguration, dummyString, dummyClass, dummyLogger},		
		};
		
		testGoodConsArgs(goodCommandLines);
	}
	
	public void testBootStateUtilNoEntries() 
	    throws SecurityException, NoSuchMethodException, IOException, 
	           ConfigurationException, ClassNotFoundException 
	{
		Class<? extends TestBootStateUtil> dummyClass = this.getClass();
	    String className = dummyClass.getName();
	    ConfigurationBuilder builder = new ConfigurationBuilder();
		Configuration dummyConfiguration = builder.buildConfiguration();
		Logger dummyLogger = Logger.getLogger(className);
		
		BootStateUtil bsu = 
			new BootStateUtil(dummyConfiguration, className, dummyClass, null);

		assertTrue(bsu.getProxyId() != null);
		assertTrue(bsu.getServiceId() != null);
		
	}	

	public void testBootStateUtilNonExistentPersistenceDir() 
		throws SecurityException, NoSuchMethodException, IOException, ConfigurationException 
	{
		Class<? extends TestBootStateUtil> dummyClass = this.getClass();
	    String className = dummyClass.getName();
	    File persistenceDir = File.createTempFile(className, ".tmp");
	    persistenceDir.delete();
	    ConfigurationBuilder builder = new ConfigurationBuilder();
	    builder.setComponentName(className);
	    builder.setPersistenceDir(persistenceDir);
		Configuration dummyConfiguration = builder.buildConfiguration();
		Logger dummyLogger = Logger.getLogger(className);
		
		// Command lines with a non-null in first three args are valid
		Object [][] goodCommandLines = {
				{dummyConfiguration, className, dummyClass, null},
				{dummyConfiguration, className, dummyClass, dummyLogger},		
		};
		testGoodConsArgs(goodCommandLines);
		
		assertTrue(persistenceDir.exists());
		persistenceDir.delete();
	}	
	
	public void testBootStateUtilExistentPersistenceDir() 
	throws SecurityException, NoSuchMethodException, IOException, ConfigurationException 
	{
		Class<? extends TestBootStateUtil> dummyClass = this.getClass();
	    String className = dummyClass.getName();
	    File persistenceDir = File.createTempFile(className, ".tmp");
	    persistenceDir.delete();
	    assertTrue(persistenceDir.mkdirs());
	    persistenceDir.deleteOnExit();
	    ConfigurationBuilder builder = new ConfigurationBuilder();
	    builder.setComponentName(className);
	    builder.setPersistenceDir(persistenceDir);
		Configuration dummyConfiguration = builder.buildConfiguration();
		Logger dummyLogger = Logger.getLogger(className);
		
		// Command lines with a non-null in first three args are valid
		Object [][] goodCommandLines = {
				{dummyConfiguration, className, dummyClass, null},
				{dummyConfiguration, className, dummyClass, dummyLogger},		
		};
		testGoodConsArgs(goodCommandLines);	 
    	persistenceDir.delete();
	}	
	
	public void testBootStateUtilWithEmptyBootState() 
	    throws SecurityException, NoSuchMethodException, IOException, 
	           ConfigurationException, ClassNotFoundException 
	{
		Class<? extends TestBootStateUtil> dummyClass = this.getClass();
	    String className = dummyClass.getName();
	    File persistenceDir = File.createTempFile(className, ".tmp");
	    persistenceDir.delete();
	    assertTrue(persistenceDir.mkdirs());
	    persistenceDir.deleteOnExit();
	    ConfigurationBuilder builder = new ConfigurationBuilder();
	    builder.setComponentName(className);
	    builder.setPersistenceDir(persistenceDir);
	    File bootStateFile = new File(persistenceDir, "boot.state");
	    bootStateFile.createNewFile();
		Configuration dummyConfiguration = builder.buildConfiguration();
		
		try {
			BootStateUtil bsu =
				new BootStateUtil(dummyConfiguration, className, dummyClass, null);
			fail("Created boot state instance with emtpy state information file");
		} catch (IOException e) {
			//ignore -- expected
		}

	}	
	
	public void testBootStateUtilWithInvalidBootState() 
    throws SecurityException, NoSuchMethodException, IOException, 
           ConfigurationException, ClassNotFoundException 
	{
		//Create boot state dir/file with default information
		Class<? extends TestBootStateUtil> dummyClass = this.getClass();
	    String className = dummyClass.getName();
	    File persistenceDir = File.createTempFile(className, ".tmp");
	    persistenceDir.delete();
	    assertTrue(persistenceDir.mkdirs());
	    persistenceDir.deleteOnExit();
	    ConfigurationBuilder builder = new ConfigurationBuilder();
	    builder.setComponentName(className);
	    builder.setPersistenceDir(persistenceDir);
		Configuration dummyConfiguration = builder.buildConfiguration();		
		new BootStateUtil(dummyConfiguration, className, dummyClass, null);
		// Mangle boot state file by writing junk into the beginning of the file
	    File bootStateFile = new File(persistenceDir, "boot.state");
	    RandomAccessFile raf = new RandomAccessFile(bootStateFile, "rws");
	    raf.seek(0);
	    raf.writeUTF("Bogus data");
	    raf.close();
	    
	    //Try to recover from bogus data
		try {
			new BootStateUtil(dummyConfiguration, className, dummyClass, null);
		} catch (StreamCorruptedException e) {
			//ignore -- expected
		}
	}	
	
	public void testBootStateUtilConsWithDefaultServiceId() 
	    throws SecurityException, NoSuchMethodException, IOException, 
	           ConfigurationException, ClassNotFoundException 
	{
		Class<? extends TestBootStateUtil> dummyClass = this.getClass();
	    String className = dummyClass.getName();
	    File persistenceDir = File.createTempFile(className, ".tmp");
	    persistenceDir.delete();
	    ConfigurationBuilder builder = new ConfigurationBuilder();
	    builder.setComponentName(className);
	    ServiceID defaultServiceId = new ServiceID(1L, 2L);
	    builder.setDefaultServiceId(defaultServiceId);
		Configuration dummyConfiguration = builder.buildConfiguration();
		Logger dummyLogger = Logger.getLogger(className);
		
		BootStateUtil bsu = 
			new BootStateUtil(dummyConfiguration, className, dummyClass, null);
		assertTrue(bsu.getServiceId().equals(defaultServiceId));
		UUID defaultProxyID = 
			new UUID(
				defaultServiceId.getMostSignificantBits(),
				defaultServiceId.getLeastSignificantBits());
		assertTrue(bsu.getProxyId().equals(defaultProxyID));
	}	
	
	public void testBootStateUtilDefaultServiceId() 
	throws SecurityException, NoSuchMethodException, IOException, 
	       ConfigurationException, ClassNotFoundException 
	{
		Class<? extends TestBootStateUtil> dummyClass = this.getClass();
	    String className = dummyClass.getName();
	    File persistenceDir = File.createTempFile(className, ".tmp");
	    persistenceDir.delete();
	    ConfigurationBuilder builder = new ConfigurationBuilder();
	    builder.setComponentName(className);
	    ServiceID defaultServiceID = new ServiceID(1L, 1L);
	    builder.setDefaultServiceId(defaultServiceID);
		Configuration dummyConfiguration = builder.buildConfiguration();
		
		BootStateUtil bsu = 
			new BootStateUtil(dummyConfiguration, className, dummyClass, null);
		assertTrue(bsu.getServiceId().equals(defaultServiceID));
		UUID proxyID = bsu.getProxyId();
		assertTrue(proxyID.getLeastSignificantBits() == defaultServiceID.getLeastSignificantBits());
		assertTrue(proxyID.getMostSignificantBits() == defaultServiceID.getMostSignificantBits());
	}	
	
	public void testBootStateUtilBadPersistenceDir() 
	throws SecurityException, NoSuchMethodException, IOException, ConfigurationException, ClassNotFoundException 
	{
		Class<? extends TestBootStateUtil> dummyClass = this.getClass();
	    String className = dummyClass.getName();
	    //create temp file -- should fail dir creation, below
	    File persistenceDir = File.createTempFile(className, ".tmp");
	    ConfigurationBuilder builder = new ConfigurationBuilder();
	    builder.setComponentName(className);
	    builder.setPersistenceDir(persistenceDir);
		Configuration dummyConfiguration = builder.buildConfiguration();
		
		try {
			new BootStateUtil(dummyConfiguration, className, dummyClass, null);
			fail("Created BootStateUtil with bad dir file: " 
				+ persistenceDir.getAbsolutePath());
		} catch (IOException e) {
			//ignore -- expected
		}
	}		
	
		
	private static void testGoodConsArgs(Object[][] goodCommandLines) 
		throws SecurityException, NoSuchMethodException 
	{
		Constructor<BootStateUtil> cons = 
			BootStateUtil.class.getConstructor(Configuration.class,
				String.class, Class.class, Logger.class);
		for (int i=0; i < goodCommandLines.length; i++) {
			try {
				cons.newInstance(goodCommandLines[i]);
			} catch (IllegalArgumentException e) {
				fail("unexpected exception: " + e.toString());
			} catch (InstantiationException e) {
				fail("unexpected exception: " + e.toString());
			} catch (IllegalAccessException e) {
				fail("unexpected exception: " + e.toString());
			} catch (InvocationTargetException e) {
				   fail("unexpected exception: " + e.getCause().toString());
			} 
		}
	}	
	
	private static class ConfigurationBuilder {		
		private String componentName = null;
		private File persistenceDir = null;
		private ServiceID defaultServiceId = null;

		ConfigurationBuilder() {
			
		}
		
		void setComponentName(String componentName) {
			this.componentName = componentName;
		}
				
		void setPersistenceDir(File persistenceDirFile) {
			this.persistenceDir = persistenceDirFile;
		}
		
		void setDefaultServiceId(ServiceID defaultServiceId) {
			this.defaultServiceId = defaultServiceId;
		}
				
		Configuration buildConfiguration() throws ConfigurationException {
			List<String> args = new ArrayList<String>();
			args.add("-");
			if(persistenceDir != null) {
				args.add(
					componentName + ".persistenceDirectory="
					    + "\"" + persistenceDir.getAbsolutePath().replace("\\", "\\\\") 
					    + "\"");
			}
			if (defaultServiceId != null){
				args.add(
						componentName  + ".defaultServiceId="
					    + "new net.jini.core.lookup.ServiceID("
					    + defaultServiceId.getMostSignificantBits()
					    + ", "
					    + defaultServiceId.getLeastSignificantBits()
                        + ")");
			}
			ConfigurationFile dummyConfiguration = 
				new ConfigurationFile(args.toArray(new String[0]));
			return dummyConfiguration;			
			
		}
	}
}
