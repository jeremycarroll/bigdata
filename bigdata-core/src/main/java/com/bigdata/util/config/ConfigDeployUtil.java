/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/

package com.bigdata.util.config;

import com.bigdata.attr.ServiceInfo;

import net.jini.config.ConfigurationException;
import net.jini.core.discovery.LookupLocator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Utility class containing a number of convenient methods that encapsulate
 * common functions related to configuration and deployment of the entity.
 * The methods of this class are <code>static</code> so that they can be
 * invoked from within a Jini configuration.
 */
public class ConfigDeployUtil {

    private static Properties deploymentProps = null;

    private static final String DEFAULT = ".default";
    private static final String STRINGVALS = ".stringvals";
    private static final String MAX = ".max";
    private static final String MIN = ".min";
    private static final String DESCRIPTION = ".description";
    private static final String TYPE = ".type";
    
    private static final String FALLBACK_FEDNAME_FORMAT = "bigdata.test.group-%s";
    
    //use current locale
    private static final NumberFormat numberFormat = NumberFormat.getInstance();

    public static String getString(String parameter) 
                             throws ConfigurationException
    {
        String value = get(parameter);
        validateString(parameter, value);
        return value;
    }

    public static String[] getStringArray(String parameter)
                               throws ConfigurationException
    {
        String[] value;
        value = validateStringArray(parameter, get(parameter));
        return value;
    }

    public static int getInt(String parameter) throws ConfigurationException {
        int value;
        value = validateInt(parameter, get(parameter));
        return value;
    }

    public static long getLong(String parameter) throws ConfigurationException
    {
        long value;
        value = validateLong(parameter, get(parameter));
        return value;
    }

    public static boolean getBoolean(String parameter) throws ConfigurationException
    {
        boolean value;
        value = validateBoolean(parameter, get(parameter));
        return value;
    }

    private static boolean validateBoolean(String parameter, String value) 
                              throws ConfigurationException {
        boolean boolValue = false;

        if( value != null && (value.equalsIgnoreCase("true")
           || value.equalsIgnoreCase("false")) )
        {
            boolValue = Boolean.parseBoolean(value);
        } else {
            throw new ConfigurationException("parameter value ["+parameter+"] "
                                             +"is neither 'true' nor 'false'");
        }
        return boolValue;
    }

    public static String getDescription(String parameter)
                             throws ConfigurationException
    {
        String value;
        value = getDeploymentProperties().getProperty(parameter + DESCRIPTION);
        return value;
    }

    public static String getType(String parameter)
                             throws ConfigurationException
    {
        String value;
        value = getDeploymentProperties().getProperty(parameter + TYPE);
        return value;
    }

    public static String getDefault(String parameter) 
                             throws ConfigurationException
    {
        String value;
        value = getDeploymentProperties().getProperty(parameter + DEFAULT);
        if (value == null) {
            throw new ConfigurationException
                          ("deployment parameter not found ["+parameter+"]");
        }
        return value;
    }

    /**
     * Returns a <code>String</code> array whose elements represent the
     * lookup service groups to discover. If the system property named
     * "federation.name" is set then that value be used; otherwise,
     * the deployment properties files will be consulted.
     *
     * @throws ConfigurationException if the groups cannot be determined.
     */
    public static String[] getGroupsToDiscover() throws ConfigurationException
    {
        String fedNameStr = getFederationName();
        return fedNameStr.split(",");
    }
    
    /**
     * Retrieve the federation name (also used as Jini group name) via this pecking order:
     * <ol>
     * <li>From the Java system property: <code>federation.name</code></li>
     * <li>From the deployment properties file. Note that a value from the deployment
     *     properties file that has not gone through token replacement is considered
     *     invalid. In this case, the next value in the pecking order is used.</li>
     * <li>Using the fallback convention: <code>bigdata.test.group-&lt;ipaddress&gt;</code></li>
     * </ol>
     * 
     * @return String the federation name
     * 
     * @throws ConfigurationException
     */
    public static String getFederationName() throws ConfigurationException
    {
        // If we have a system property override, use that.
        String fedName = System.getProperty("federation.name");
        
        // If not, look in the deploy properties
        if(fedName == null) {
            fedName = getString("federation.name");
        }
        
        // If not set in the deploy properties, then use the fallback name of 
        // "bigdata.test.group-<ipaddress>". This is primarily to support test 
        // environments and we should never get this far in production.
        if (fedName == null || fedName.length() == 0) {
            try {
                String ipAddress = NicUtil.getIpAddress("default.nic", "default", true);
                fedName = String.format(FALLBACK_FEDNAME_FORMAT, ipAddress);
            }
            catch (Exception e) {
                throw new ConfigurationException("Error retrieving IP address while constructing fallback federation name.", e);
            }
        } 
        
        return fedName;
    }

    /**
     * Returns an array of <code>LookupLocator</code> instances that can
     * each be used to discover a specific lookup service.
     *
     * @throws ConfigurationException if the locators cannot be determined.
     */
    public static LookupLocator[] getLocatorsToDiscover()
                                      throws ConfigurationException
    {
        return new LookupLocator[]{};
    }

    /** 
     * Returns an instance of the <code>ServiceInfo</code> attribute class,
     * initialized to the values specified in the deployment properties
     * files.
     */
    public static ServiceInfo initServiceInfo(UUID source, String serviceName)
                  throws SocketException, IOException, ConfigurationException
    {
        ServiceInfo serviceInfo = new ServiceInfo();
        serviceInfo.source = source;
        serviceInfo.serviceName = serviceName;

        serviceInfo.inetAddresses = NicUtil.getInetAddressMap();

        // Get the common token that all services running on the same
        // node agree on. Use the MAC address or IP address as default token
        String nodeNicName = getString("node.serviceNetwork");
        String nodeIp = NicUtil.getIpAddress("default.nic",nodeNicName,false);
        serviceInfo.nodeToken = NicUtil.getMacAddress(nodeIp);
        if(serviceInfo.nodeToken == null) serviceInfo.nodeToken = nodeIp;

        serviceInfo.nodeId = null;//not set until a node service exists
        serviceInfo.nodeName = getString("node.name");

        serviceInfo.uNumber = getInt("node.uNumber");

        serviceInfo.rack   = getString("node.rack");
        serviceInfo.cage   = getString("node.cage");
        serviceInfo.zone   = getString("node.zone");
        serviceInfo.site   = getString("node.site");
        serviceInfo.region = getString("node.region");
        serviceInfo.geo    = getString("node.geo");

        return serviceInfo;
    }


    private static String get(String parameter) throws ConfigurationException {
        String value;
        value = getDeploymentProperties().getProperty(parameter);
        if (value == null) value = getDefault(parameter);
        return value;
    }

    private static void validateString(String parameter, String value) 
                            throws ConfigurationException
    {
        String validValuesStr = 
            (String) getDeploymentProperties().get(parameter + STRINGVALS);

        if (validValuesStr != null) {
            String[] validValues = validValuesStr.split(",");
            if (!Arrays.asList(validValues).contains(value)) {
                throw new ConfigurationException
                              ("invalid string parameter ["+parameter+"] in "
                               +"list ["+validValuesStr+"]");
            }
        }
        return;
    }

    private static String[] validateStringArray(String parameter, String value)
                                throws ConfigurationException
    {
        String validValuesStr = 
            (String)(getDeploymentProperties().get(parameter + STRINGVALS));
        String[] values = value.split(",");

        if (validValuesStr != null) {
            String[] validValues = validValuesStr.split(",");
            List validValuesList = Arrays.asList(validValues);
            for (int i=0; i<values.length; i++) {
                if (!validValuesList.contains(values[i])) {
                    throw new ConfigurationException
                              ("invalid string parameter ["+parameter+"] in "
                               +"list "+validValuesList);
                }
            }
        }
        return values;
    }

    private static int validateInt(String parameter, String strvalue)
                           throws ConfigurationException
    {
        String maxString = (String)(getDeploymentProperties().get(parameter + MAX));
        String minString = (String)(getDeploymentProperties().get(parameter + MIN));

        int value = str2int(strvalue);

        if (maxString != null) {
            try {
            	int max = numberFormat.parse(maxString).intValue();          
	            if (value > max) {
	                throw new ConfigurationException("parameter ["+parameter+"] "
	                                                 +"exceeds maximum ["+max+"]");
	            }
            } catch (ParseException e) {
            	throw new NumberFormatException(
            		"Invalid maximum integer for parameter: " + parameter);
         	
            }
        }
        
        if (minString != null) {
            try {
            	int min = numberFormat.parse(minString).intValue();          
	            if (value < min) {
	                throw new ConfigurationException("parameter ["+parameter+"] "
	                    + "is less than manimum ["+min+"]");
	            }
            } catch (ParseException e) {
            	throw new NumberFormatException(
                		"Invalid minimum integer for parameter: " + parameter);         	
            }
        }
        
        return value;
    }

    private static long validateLong(String parameter, String strvalue) 
                            throws ConfigurationException
    {
        String maxString = (String)(getDeploymentProperties().get(parameter + MAX));
        String minString = (String)(getDeploymentProperties().get(parameter + MIN));

        long value = str2long(strvalue);

        if (maxString != null) {
            try {
            	long max = numberFormat.parse(maxString).longValue();          
	            if (value > max) {
	                throw new ConfigurationException("parameter ["+parameter+"] "
	                                                 +"exceeds maximum ["+max+"]");
	            }
            } catch (ParseException e) {
            	throw new NumberFormatException(
            		"Invalid maximum long for parameter: " + parameter);
         	
            }
        }
        if (minString != null) {
            try {
            	long min = numberFormat.parse(minString).longValue();          
	            if (value < min) {
	                throw new ConfigurationException("parameter ["+parameter+"] "
	                    + "is less than manimum ["+min+"]");
	            }
            } catch (ParseException e) {
            	throw new NumberFormatException(
                		"Invalid minimum long for parameter: " + parameter);         	
            }            
        }
        return value;
    }

    private static File getPropertiesPath() {
        File rootDir = new File("/opt/bigdata"); //real installation
        String appHome = System.getProperty("appHome");//pstart
        String appDotHome = System.getProperty("app.home");//build.xml

        if(appHome != null) {
            rootDir = new File(appHome);
        } else if(appDotHome != null) {
            rootDir = new File(appDotHome);
        }

        File retDir = new File(rootDir,"var/config/deploy");

        //eclipse
        if( !retDir.exists() ) {
            retDir = new File("src/main/deploy/var/config/deploy");
        }
        return retDir;
    }

    private static void loadDeployProps(Properties deployProps) {
        loadDefaultProps(deployProps);
        loadOverrideProps(deployProps);
    }

    private static void loadDefaultProps(Properties deployProps) {
    	loadPropsInternal("default-deploy.properties", deployProps, true);
    }

    private static void loadOverrideProps(Properties deployProps) {
    	loadPropsInternal("deploy.properties", deployProps, false);
    }
    
    private static void loadPropsInternal(String propFileName, 
    	Properties deployProps,	boolean prtinTrace) 
    {    
        FileInputStream fis = null;
        try {
            File flnm = new File(getPropertiesPath(), propFileName);
            fis = new FileInputStream(flnm);
            deployProps.load(fis);
        } catch (Exception ex) {
            if (prtinTrace) ex.printStackTrace();
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioex) { /* swallow */ }
            }
        }
    }


    private static int str2int(String argx) {
    	Number n = null;
    	try {	
    		//TODO - truncation can occur -- check for overflow
    		n = numberFormat.parse(argx);
    	} catch (ParseException e) {
    		throw new NumberFormatException("Invalid integer: " + argx);
    	}
    	return n.intValue();
    }

    private static long str2long(String argx) {
    	Number n = null;
    	try {	
    		//TODO - truncation can occur -- check for overflow
    		n = numberFormat.parse(argx);
    	} catch (ParseException e) {
    		throw new NumberFormatException("Invalid long: " + argx);
    	}
    	return n.longValue();
    }
    
    /**
     * Returns reference to <code>deploymenyProps</code> field. If null, then the
     * field is populated by looking for the default and override properties files 
     * (defined in <code>loadDefaultProps</code> and <code>loadOverrideProps</code>). 
     * This method is synchronized in order to ensure that the returned reference is
     * a singleton instance.
     * Note: This method should be private, but is needed by the unit test in order
     * to access and modify the <code>Properties</code> method.
     * @return Properties instance containing the default and user-defined overrides 
     *     for configuration properties.
     */
    synchronized static Properties getDeploymentProperties() {    
	    if(deploymentProps == null) {
	        deploymentProps = new Properties();
	        loadDeployProps(deploymentProps);
	    }
	    return deploymentProps;
    }
    
    /**
     * Convenience method intended for use by unit tests only.
     * @param properties Sets <code>Properties</code> object 
     */
    synchronized static void setDeploymentProperties(Properties properties) {    
    	deploymentProps = properties;
    }
    
}
