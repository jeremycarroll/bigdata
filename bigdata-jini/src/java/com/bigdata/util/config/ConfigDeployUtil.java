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

    private static final String FALLBACK_FEDNAME_FORMAT =
                                    "bigdata.test.group-%s";
    
    //use current locale
    private static final NumberFormat numberFormat =
                                          NumberFormat.getInstance();

    //prevent instantiation
    private ConfigDeployUtil() {
        throw new AssertionError("ConfigDeployUtil cannot be instantiated");
    }

    /**
     * Returns the <code>String</code> value associated with the given
     * parameter in the configured properties file. The value, if any,
     * returned will be:
     *
     * 1) the explicitly defined value for the property, or
     * 2) the default value for that property, if a default exists.
     *
     * If the provided parameter has an associated set of valid entries in the
     * properties files, then the value obtained, above, will be validated 
     * against those entries. If either the value (explicit or default) can't
     * be found or fails the validation check, a
     * <code>ConfigurationException</code> will be thrown.
     *
     * @param parameter The property name to lookup
     *
     * @return the <code>String</code> value associated the requested
     *         parameter, if available
     *
     * @throws ConfigurationException if the parameter value (explicit or
     *         default) was not defined.
     */
    public static String getString(String parameter) 
                             throws ConfigurationException
    {
        String value = get(parameter);
        validateString(parameter, value);
        return value;
    }

    /**
     * Returns the <code>String[]</code> value associated with the given
     * parameter in the configured properties file. The value, if any,
     * returned will be:
     *
     * 1) the explicitly defined value for the property, or
     * 2) the default value for that property, if a default exists.
     *
     * If the provided parameter has an associated set of valid entries in the
     * properties files, then the component values obtained, above, will be
     * validated against those entries. If either the value (explicit or
     * default) can't be found or fails the validation check, a
     * <code>ConfigurationException</code> will be thrown.
     *
     * @param parameter The property name to lookup
     *
     * @return the <code>String[]</code> value associated the requested
     *         parameter, if available
     *
     * @throws ConfigurationException if the parameter value (explicit or
     *         default) was not defined.
     */    
    public static String[] getStringArray(String parameter)
                               throws ConfigurationException
    {
        String[] value;
        value = validateStringArray(parameter, get(parameter));
        return value;
    }

    /**
     * Returns the <code>int</code> value associated with the given parameter 
     * in the configured properties file. The value, if any, returned will be:
     *
     * 1) the explicitly defined value for the property, or
     * 2) the default value for that property, if a default exists.
     *
     * If the provided parameter has an associated set of valid entries in the
     * properties files, then the value obtained, above, will be validated 
     * against those entries. If either the value (explicit or default) can't
     * be found or fails the validation check, a
     * <code>ConfigurationException</code> will be thrown.
     *
     * @param parameter The property name to lookup
     *
     * @return the <code>int</code> value associated the requested parameter,
     *         if available
     *
     * @throws ConfigurationException if the parameter value (explicit or
     *         default) was not defined.
     */    
    public static int getInt(String parameter) throws ConfigurationException {
        int value;
        value = validateInt(parameter, get(parameter));
        return value;
    }

    /**
     * Returns the <code>long</code> value associated with the given parameter 
     * in the configured properties file. The value, if any, returned will be:
     *
     * 1) the explicitly defined value for the property, or
     * 2) the default value for that property, if a default exists.
     *
     * If the provided parameter has an associated set of valid entries in the
     * properties files, then the value obtained, above, will be validated 
     * against those entries. If either the value (explicit or default) can't
     * be found or fails the validation check, a
     * <code>ConfigurationException</code> will be thrown.
     *
     * @param parameter The property name to lookup
     *
     * @return the <code>long</code> value associated the requested parameter,
     *         if available
     *
     * @throws ConfigurationException if the parameter value (explicit or
     *         default) was not defined.
     */    
    public static long getLong(String parameter) throws ConfigurationException
    {
        long value;
        value = validateLong(parameter, get(parameter));
        return value;
    }

    /**
     * Returns the <code>boolean</code> value associated with the given
     * parameter in the configured properties file. The value, if any,
     * returned will be:
     *
     * 1) the explicitly defined value for the property, or
     * 2) the default value for that property, if a default exists.
     *
     * If the provided parameter has an associated set of valid entries in the
     * properties files, then the value obtained, above, will be validated 
     * against those entries. If either the value (explicit or default) can't
     * be found or fails the validation check, a
     * <code>ConfigurationException</code> will be thrown.
     *
     * @param parameter The property name to lookup
     *
     * @return the <code>boolean</code> value associated the requested
     *         parameter, if available
     *
     * @throws ConfigurationException if the parameter value (explicit or
     * default) was not defined.
     */    
    public static boolean getBoolean(String parameter) 
                              throws ConfigurationException
    {
        boolean value;
        value = validateBoolean(parameter, get(parameter));
        return value;
    }

    /**
     * Returns the description value associated with the given parameter 
     * in the configured properties file. The method returns <code>null</code>
     * if the property is not found.
     *
     * @param parameter The property name to lookup
     *
     * @return the <code>String</code> value associated the requested
     *         parameter, if available. Otherwise return <code>null</code>
     *
     * @throws ConfigurationException if there was a problem accessing the
     *         parameter value.
     */    
    public static String getDescription(String parameter)
                             throws ConfigurationException
    {
        String value;
        value = getDeploymentProperties().getProperty(parameter + DESCRIPTION);
        return value;
    }

    /**
     * Returns the type value associated with the given parameter 
     * in the configured properties file. The method returns <code>null</code>
     * if the property is not found.
     *
     * @param parameter The property name to lookup
     *
     * @return the <code>String</code> value of the type associated the
     *         requested parameter, if available. Otherwise return
     *         <code>null</code>
     *
     * @throws ConfigurationException if there was a problem accessing the
     *         parameter value.
     */    
    public static String getType(String parameter)
                             throws ConfigurationException
    {
        String value;
        value = getDeploymentProperties().getProperty(parameter + TYPE);
        return value;
    }

    /**
     * Returns the default value associated with the given parameter 
     * in the configured properties file.
     *
     * @param parameter The property name to lookup
     *
     * @return the <code>String</code> value of the type associated the
     *         requested parameter, if available. Otherwise return
     *         <code>null</code>
     *
     * @throws ConfigurationException if no default value was specified
     */        
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
     * Returns a <code>String</code> array whose elments represent the
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
     * Returns the federation name (Jini group name) via the following 
     * selection order:
     * <ol>
     * <li>From the Java system property: <code>federation.name</code></li>
     * <li>From the deployment properties file. Note that a value from the
     *     deployment properties file that has not gone through token
     *     replacement is considered invalid. In this case, the next value
     *     in the selection order is used.</li>
     * <li>Using the fallback convention:
     *     <code>bigdata.test.group-&lt;ipaddress&gt;</code></li>
     * </ol>
     * 
     * @return String value representing the federation name
     * 
     * @throws ConfigurationException
     */
    public static String getFederationName() throws ConfigurationException {

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
                String ipAddress = 
                    NicUtil.getIpAddress("default.nic", "default", true);
                fedName = String.format(FALLBACK_FEDNAME_FORMAT, ipAddress);
            }
            catch (Exception e) {
                throw new ConfigurationException
                              ("error retrieving IP address while "
                               +"constructing fallback federation name.", e);
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
            String[] validValues = (validValuesStr.trim()).split(",");
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
            String[] validValues = (validValuesStr.trim()).split(",");
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

//NOTE: there seems to be a problem with NumberFormat.parse; in particular,
//      with parsing strings representing integers. During testing
//      there were numerous occasions where, when parsing a string value
//      from the properties (from either default-deploy.properties or
//      from deploy.properties), either a NumberFormatException or
//      a ParseException would occur; even when the value being parsed
//      was valid. These exceptions, which occurred randomly, typically
//      would indicate that value being parsed was the empty string ("")
//      for the case of a NumberFormatException, or the resulting parsed
//      max (or min) value did not satisfy the desired criteria. For
//      example, when a strvalue of "2181" was input, and the maximum
//      value retrieved from the properties was "65353", upon parsing the
//      string "65353", the value 1024 was returned; thus, because 1024
//      is less than 2181, a ParseException was thrown. In other cases,
//      although a NumberFormatException was thrown by the call to
//      NumberFormat.parse because that method interpretted the string
//      "65353" as the empty string.
//
//      Upon doing a search of the various bug databases and related
//      user reports, there seems to be some indication that the
//      parse method may at some point invoke the indexOf method on
//      the string that is being parsed. Thus, the problem being
//      described here may be related to JDK bug described at the url,
//
//      http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6935535
//
//      (Or it may be related to the locale being used by the tests that
//      encountered this issue?)
//
//      As a work around, the validateInt and validateLong methods each
//      parse the given string using Integer.parseInt and Long.parseLong
//      respectively; at least until this issue is resolved.

    private static int validateInt(String parameter, String strvalue)
                           throws ConfigurationException
    {
        String maxString =
            (String)(getDeploymentProperties().get(parameter + MAX));
        String minString =
            (String)(getDeploymentProperties().get(parameter + MIN));

        int value = str2int(strvalue);

        if (maxString != null) {
/* see note ************************************************
            try {
                int max = numberFormat.parse(maxString.trim()).intValue();
            } catch (ParseException e) {
                throw new NumberFormatException
                              ("invalid maximum integer for parameter: "
                               +parameter);
            }
************************************************************* */
int max = Integer.parseInt(maxString.trim());
            if (value > max) {
                throw new ConfigurationException
                          ("parameter ["+parameter+"] exceeds maximum "
                           +"["+strvalue+" > "+max
                           +" (maxString="+maxString+")]");
            }
        }
        if (minString != null) {
/* see note ************************************************
            try {
                int min = numberFormat.parse(minString.trim()).intValue();
            } catch (ParseException e) {
                throw new NumberFormatException
                              ("invalid minimum integer for parameter: "
                               +parameter);         	
            }
************************************************************* */
int min = Integer.parseInt(minString.trim());
            if (value < min) {
                throw new ConfigurationException
                          ("parameter ["+parameter+"] is less than minimum "
                           +"["+strvalue+" < "+min
                           +" (minString="+minString+")]");
            }
        }

        return value;
    }

    private static long validateLong(String parameter, String strvalue) 
                            throws ConfigurationException
    {
        String maxString =
            (String)(getDeploymentProperties().get(parameter + MAX));
        String minString =
            (String)(getDeploymentProperties().get(parameter + MIN));

        long value = str2long(strvalue);

        if (maxString != null) {
/* see note ************************************************
            try {
                long max = numberFormat.parse(maxString.trim()).longValue();
            } catch (ParseException e) {
                throw new NumberFormatException
                              ("invalid maximum long for parameter: "
                               +parameter);
            }
************************************************************* */
long max = Long.parseLong(maxString.trim());
            if (value > max) {
                throw new ConfigurationException
                          ("parameter ["+parameter+"] exceeds maximum "
                           +"["+strvalue+" > "+max
                           +" (maxString="+maxString+")]");
            }
        }
        if (minString != null) {
/* see note ************************************************
            try {
                long min = numberFormat.parse(minString.trim()).longValue();
            } catch (ParseException e) {
                throw new NumberFormatException
                              ("invalid minimum long for parameter: "
                               +parameter);         	
            }            
************************************************************* */
long min = Long.parseLong(minString.trim());
            if (value < min) {
                throw new ConfigurationException
                          ("parameter ["+parameter+"] is less than minimum "
                           +"["+strvalue+" < "+min
                           +" (minString="+minString+")]");
            }
        }
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
            String tmpPath = "bigdata-jini" + File.separator + "src"
                                            + File.separator + "java"
                                            + File.separator + "com"
                                            + File.separator + "bigdata"
                                            + File.separator + "util"
                                            + File.separator + "config";
            //first resolve by user.dir
            retDir = new File( (new File(tmpPath)).getAbsolutePath() );
            if( !retDir.exists() ) {//fallback assumes appDotHome
                retDir = new File( rootDir, tmpPath );
            }
//maven_scaleout - retDir = new File("src/main/deploy/var/config/deploy");
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

//BTM - NOTE: the orginal versions of str2int and str2long were
//BTM -       replaced by Bob Resendes in changeset 3721 with the
//BTM -       commented out versions shown below for reference.
//BTM -       During testing for the ClientService smart proxy
//BTM -       conversion work, the test TestBigdataClient failed
//BTM -       with a NumberFormatException from str2long when
//BTM -       when the new version str2long was used. Thus, at
//BTM -       least temporarily, until the problem can be 
//BTM -       diagnosed and fixed, the old versions of those
//BTM -       methods are still being used below.
//BTM     private static int str2int(String argx) {
//BTM         Number n = null;
//BTM         try {	
//BTM //TODO - truncation can occur -- check for overflow
//BTM             n = numberFormat.parse(argx);
//BTM         } catch (ParseException e) {
//BTM             throw new NumberFormatException("invalid integer: "+argx);
//BTM         }
//BTM         return n.intValue();
//BTM     }
//BTM 
//BTM     private static long str2long(String argx) {
//BTM         Number n = null;
//BTM         try {	
//BTM //TODO - truncation can occur -- check for overflow
//BTM             n = numberFormat.parse(argx);
//BTM         } catch (ParseException e) {
//BTM             throw new NumberFormatException("invalid long: "+argx);
//BTM         }
//BTM         return n.longValue();
//BTM     }
    private static int str2int(String argx) {
        long l;

        if( argx.trim().equals(Integer.MAX_VALUE) ) return Integer.MAX_VALUE;
        if( argx.trim().equals(Integer.MIN_VALUE) ) return Integer.MIN_VALUE;

        l = str2long(argx);
        if (l < Integer.MAX_VALUE && l > Integer.MIN_VALUE) {
            return (int) l;
        } else {
            throw new NumberFormatException("Invalid number:"+argx
                                            +"  --number out of range");
        }
    }

    private static long str2long(String argx) {

        int minDigitNumBetwnComma = 3;

        String arg = argx.trim();
        arg = arg.replaceAll("\"", ""); // strip all quotes
        int sz = arg.length();

        if( arg.equals("Long.MAX_VALUE") ) return Long.MAX_VALUE;

        if( arg.equals("Long.MIN_VALUE") ) return Long.MIN_VALUE;

        int asterPos = -1;
        String arg1 = null;
        String arg2 = null;
        if( (asterPos = arg.indexOf("*")) != -1) {
            int dotPos = -1;
            arg1 = arg.substring(0, asterPos).trim();
            int denom1 = 1;
            if( (dotPos = arg1.indexOf(".")) != -1) {
                StringBuffer tmpBuf = new StringBuffer("1");
                int hitNumber = 0;
                for (int i = dotPos + 1; i < (arg1.length() - dotPos); i++) {
                    if( Character.isDigit(arg1.charAt(i)) ) {
                        tmpBuf.append("0");
                    } else {
                        break;
                    }
                }
                denom1 = Integer.valueOf(tmpBuf.toString());
                arg1 = arg1.substring(0, dotPos) + arg1.substring(dotPos + 1);
            }

            arg2 = arg.substring(asterPos + 1).trim();
            int denom2 = 1;
            if( (dotPos = arg2.indexOf(".")) != -1) {
                StringBuffer tmpBuf = new StringBuffer("1");
                for(int i = dotPos + 1; i <= (arg2.length() - dotPos); i++) {
                    tmpBuf.append("0");
                }

                denom2 = Integer.valueOf(tmpBuf.toString());
                arg2 = arg2.substring(0, dotPos) + arg2.substring(dotPos + 1);
            }

            long numerator = str2long(arg1) * str2long(arg2);
            long denom = (denom1 * denom2);

            if (numerator % denom != 0) {
                throw new NumberFormatException(" Bad value passed in:" +
                                                ((double) (numerator) /
                                                 denom) +
                                                ", expecting a long");
            }
            return (numerator / denom);
        }

        char unit = arg.charAt(sz - 1);

        String valScalar = arg.substring(0, (sz - 1)).trim();

        long factor = 0l;

        switch (Character.toUpperCase(unit)) {

            case 'G':
                factor = 1000000000l;
                break;
            case 'M':
                factor = 1000000l;
                break;
            case 'K':
                factor = 1000l;
                break;
            case 'B':
                char unitPre = arg.charAt(sz - 2);
                if (Character.isDigit(unitPre)) {
                    factor = -1l;
                } else {
                    factor =
                        (Character.toUpperCase(unitPre) ==
                         'G' ? 1000000000l : (Character.toUpperCase(unitPre) ==
                                          'M' ? 1000000l : (Character.
                                                            toUpperCase
                                                            (unitPre) ==
                                                            'K' ? 1000l :
                                                            -1l)));
                    valScalar = arg.substring(0, (sz - 2)).trim();
                }
                break;

            default:
                if (Character.isDigit(unit)) {
                    factor = 1l;
                    valScalar = arg;
                }
        }
        if (factor == -1l) {
            throw new NumberFormatException("Invalid number:" + arg);
        }

        int comaPos = -1;
        if( (comaPos = valScalar.indexOf(',')) != -1) {
            if(valScalar.indexOf('.') != -1) {
                throw new NumberFormatException("Invalid number:"+arg
                                                +" both \",\" and decimal "
                                                +"point are not supported");
            }
            if( comaPos != 0 && comaPos != (valScalar.length() - 1) ) {
                String[]spltByComa = valScalar.split(",");
                valScalar = "";
                for (int i = spltByComa.length - 1; i >= 0; i--) {
                    if(i > 0 && spltByComa[i].length() < minDigitNumBetwnComma)
                    {
                        throw new NumberFormatException("Invalid number:"+arg
                                                        +"  unexpected comma "
                                                        +"format");
                    }
                    valScalar = spltByComa[i] + valScalar;
                }
            } else {
                throw new NumberFormatException("Invalid number:\"" +arg
                                                +"\" -unexpected comma in "
                                                +"position: "+comaPos);
            }
        }

        int decimalPos = -1;
        String valMultiplByFactor = null;
        int numZero = 0;
        try {
            if( (decimalPos = valScalar.indexOf('.')) != -1) {
                if (decimalPos != valScalar.lastIndexOf('.')) {
                    throw new NumberFormatException("Invalid number:"
                                                    +valScalar
                                                    +"  --invalid decimal "
                                                    +"number, bad value");
                }

                String facStr = String.valueOf(factor);
                int numZeroFactor = facStr.length() - 1;
                int numDigitsAfterDecimal =
                    valScalar.length() - decimalPos - 1;
                int countZero = 0;
                for(int i = valScalar.length() - 1; i > decimalPos; i--) {

                    if (valScalar.charAt(i) != '0')
                        break;
                    --numDigitsAfterDecimal;
                    countZero++;
                }
                numZero = numZeroFactor - numDigitsAfterDecimal;
                if (numZero == numDigitsAfterDecimal) {
                    numZero = 0;
                }
                if(numZero < 0) {
                    throw new NumberFormatException("Invalid number:"
                                                    +valScalar
                                                    +"  --invalid decimal "
                                                    +"number, numzero="
                                                    + numZero);
                }

                if(numZero >= 0) {
                    StringBuffer tmpStrNum =
                        new StringBuffer(20).
                        append(valScalar.substring(0, decimalPos)).
                        append(valScalar.substring(decimalPos + 1,
                                                   decimalPos + 1 +
                                                   numDigitsAfterDecimal));
                    for(int i=0; i<numZero; i++) {
                        tmpStrNum.append('0');
                    }
                    valMultiplByFactor = tmpStrNum.toString();
                }

            }
        } catch(NumberFormatException nfe) {
            throw new NumberFormatException("Invalid number:" +valScalar
                                            +"  --invalid decimal number, "
                                            +"numZero="+numZero);
        }

        long ret = -1l;

        Long ll = ((decimalPos != -1) ? Long.valueOf(valMultiplByFactor)
                   : (Long.valueOf(valScalar) * factor));
        if( (ret = Long.valueOf(ll)) >= Long.MAX_VALUE
            || ret <= Long.MIN_VALUE)
        {
            throw new NumberFormatException("Invalid number:"+arg
                                            +"  --absolute value of number "
                                            +"too big");
        }
        return ret;
    }

    /**
     * Returns a reference to the <code>deploymenyProps</code> field.
     * If <code>null</code>, then the field is populated by looking for
     * the default and override properties files (defined in
     * <code>loadDefaultProps</code> and <code>loadOverrideProps</code>).
     *
     * This method is synchronized in order to ensure that the returned
     * reference is a singleton instance.
     *
     * Note: This method should be private, but is needed by the unit
     * tests in order to access and modify the underlying
     * <code>Properties</code> object.
     *
     * @return Properties instance containing the configuration properties.
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
     *
     * @param properties Sets <code>Properties</code> object 
     */
    synchronized static void setDeploymentProperties(Properties properties) {
    	deploymentProps = properties;
    }
}
