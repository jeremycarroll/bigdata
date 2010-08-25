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
package com.bigdata.boot.launcher;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;
import java.util.TreeMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Class that reads the XML configuration file and stores the processed
 * configuration information for convenient access later.
 */
class ConfigReader {

    private final File file;
    private final ConfigurationPropertyMap rootPropertyMap;
    private final Map<String,ProcessConfiguration> processMap;
    private final List<String> bootProcesses;

    public ConfigReader(File f, ConfigurationPropertyMap rootPropertyMap)
               throws IOException, SAXException
    {
        this(null, f, rootPropertyMap,
             new TreeMap<String,ProcessConfiguration>(),
             new ArrayList<String>());
    }

    /**
     * Constructor used for parsing included configuration files.
     *
     * @throws IllegalArgumentException if the file is not a recognized
     *         file type.
     */
    private ConfigReader(ParseHandler                      parentHandler,
                         File                              f,
                         ConfigurationPropertyMap          rootPropertyMap,
                         Map<String, ProcessConfiguration> processMap,
                         List<String>                      bootProcesses)
                throws IOException, SAXException
    {
        this.file = f;
        this.rootPropertyMap = rootPropertyMap;
        this.processMap = processMap;
        this.bootProcesses = bootProcesses;

        if( file.getName().endsWith(".xml") ) {
            try {
                SAXParserFactory factory = SAXParserFactory.newInstance();
                SAXParser saxParser = factory.newSAXParser();
                saxParser.parse(f, new ParseHandler(parentHandler));
            } catch(ParserConfigurationException e) {
                throw new RuntimeException(e);
            } catch(SAXException e) {
                Exception cause = e.getException();
                if( (cause != null) && (cause instanceof IOException) ) {
                    throw (IOException)cause;
                } else {
                    throw e;
                }
            }
        } else if( file.getName().endsWith(".properties") ||
                   file.getName().endsWith(".config") )
        {
            InputStream is = new java.io.FileInputStream(file);
            try {
                Properties props = new Properties();
                props.load(is);
                for(String key : props.stringPropertyNames())
                    rootPropertyMap.setProperty(key, props.getProperty(key));
            } finally {
                is.close();
            }
        } else {
            throw new IllegalArgumentException("Unknown file type [file="
                                               +file.getName()+"]");
        }
    }

    /** Gets the property map that is used by the boot launcher. */
    public ConfigurationPropertyMap getBootPropertyMap() {
        return rootPropertyMap;
    }

    /**
     * Returns a collection containing ProcessConfiguration objects for
     * all the processes that were defined in the configuration file.
     */
    public Collection<ProcessConfiguration> getProcessList() {
        return processMap.values();
    }

    /**
     * Returns a list whose elements are the process tags associated with
     * each of the boot processes.
     */
    public List<String> getBootProcesses() {
        return bootProcesses;
    }


    private class ParseHandler extends DefaultHandler {

        private final Stack<String> elementStack = new Stack<String>();
        private final Stack<ConfigurationPropertyMap> configStack =
                          new Stack<ConfigurationPropertyMap>();
        private ProcessConfiguration currentProcessConfig;

        ParseHandler(ParseHandler parentHandler) {
            if(parentHandler != null) {
                // The ParseHandler for an included file. Copy the relevant
                // info from the parent ParseHandler.
                this.currentProcessConfig = parentHandler.currentProcessConfig;
            }
        }

        public void startDocument() throws org.xml.sax.SAXException {
            if( !elementStack.empty() ) {
                throw new SAXException("unexpected start of doc");
            }
            elementStack.push("ROOT");
            configStack.push(rootPropertyMap);
        }

        public void endDocument() throws org.xml.sax.SAXException {
            if(elementStack.empty() || (! elementStack.pop().equals("ROOT"))) {
                throw new SAXException("unexpected end of doc");
            }
            configStack.pop();
        }

        public void startElement(String     uri,
                                 String     localName,
                                 String     qName,
                                 Attributes attrs) throws SAXException
        {
            if( elementStack.isEmpty() ) {
                throw new SAXException("unexpected element");
            }
            String eName = localName;
            if( eName.equals("") ) eName = qName;

            if( eName.equalsIgnoreCase("boot") ) {

                // Validate the parent element
                if( !elementStack.peek().equals("ROOT") ) {
                    throw new SAXException("misplaced 'boot' tag");
                }

                // No attributes

            } else if( eName.equalsIgnoreCase("property") ) {

                // Validate the parent element
                if( !elementStack.peek().equals("boot") &&
                    !elementStack.peek().equals("process") )
                {
                    throw new SAXException("misplaced 'property' tag");
                }

                ConfigurationPropertyMap c = configStack.peek();
                String name = attrs.getValue("name");
                String value = attrs.getValue("value");

                if( (name == null) || (value == null) ) {
                    throw new SAXException("'property' tag is missing "
                                           +"'name' or 'value' attribute");
                }
                c.setProperty(name, substituteValues(value));

            } else if( eName.equalsIgnoreCase("javaprop") ) {

                // Validate the parent element
                if( !elementStack.peek().equals("boot") &&
                    !elementStack.peek().equals("process") )
                {
                    throw new SAXException("misplaced 'javaprop' tag");
                }

                ConfigurationPropertyMap c = configStack.peek();
                String name = attrs.getValue("name");
                String value = attrs.getValue("value");

                if( (name == null) || (value == null) ) {
                    throw new SAXException("'javaprop' tag is missing "
                                           +"'name' or 'value' attribute");
                }
                c.setJavaProp(name, substituteValues(value));

            } else if( eName.equalsIgnoreCase("process") ) {

                // Validate the parent element
                if( (currentProcessConfig != null) ||
                    ( !elementStack.peek().equalsIgnoreCase("boot")) )
                {
                    throw new SAXException("misplaced 'process' tag");
                }

                // Duplicate the current configuration and push it on the
                // config stack
                try {
                    configStack.push
                        ((ConfigurationPropertyMap)configStack.peek().clone());
                } catch(CloneNotSupportedException e) {
                    throw new RuntimeException("clone failed", e);
                }

                String cls = attrs.getValue("class");
                String tag = attrs.getValue("tag");

                if( (cls == null) || (tag == null) ) {
                    throw new SAXException("'process' tag is missing "
                                           +"'class' or 'tag' attribute");
                }

                boolean performHandshake = true;
                String handshakeStr = attrs.getValue("handshake");

                if(handshakeStr != null) {
                    if( handshakeStr.equalsIgnoreCase("true") ) {
                        performHandshake = true;
                    } else if( handshakeStr.equalsIgnoreCase("false") ) {
                        performHandshake = false;
                    } else {
                        throw new SAXException("invalid value for "
                                               +"'handshake' attribute");
                    }
                }

                long stopTimeout = 60000;
                String stopTimeoutStr = attrs.getValue("stopTimeout");

                if(stopTimeoutStr != null) {
                    try {
                        stopTimeout = Long.parseLong(stopTimeoutStr) * 1000;
                        if(stopTimeout < 0) throw new NumberFormatException();
                    } catch(NumberFormatException e) {
                        throw new SAXException("invalid value for the "
                                               +"'stopTimeout' attribute");
                    }
                }

                this.currentProcessConfig = 
                    new ProcessConfiguration(tag, cls, configStack.peek(),
                                             performHandshake, stopTimeout);

            } else if( eName.equalsIgnoreCase("include") ) {

                // Validate the parent element
                if( !elementStack.peek().equals("boot") &&
                    !elementStack.peek().equals("process") )
                {
                    throw new SAXException("misplaced 'include' tag");
                }

                String includeFileName = attrs.getValue("file");

                if(includeFileName == null) {
                    throw new SAXException
                                  ("'include' tag missing 'file' attribute");
                }

                boolean required = true;
                String requiredStr = attrs.getValue("required");

                if(requiredStr != null) {
                    if( requiredStr.equalsIgnoreCase("true") ) {
                        required = true;
                    } else if( requiredStr.equalsIgnoreCase("false") ) {
                        required = false;
                    } else {
                        throw new SAXException
                                    ("invalid value for 'required' attribute");
                    }
                }

                File subFile = new File(file.getParentFile(),
                                        substituteValues(includeFileName));
                try {
                    // Create a new config reader and pass the current
                    // ConfigurationPropertyMap as the root config
                    ConfigReader subConfig = 
                        new ConfigReader(this, subFile, configStack.peek(),
                                         processMap, bootProcesses);
                } catch(IOException e) {
                    if( (e instanceof java.io.FileNotFoundException) &&
                        (required == false) )
                    {
                        // Silently ignore
                    } else {
                        throw new SAXException(e.getMessage(), e);
                    }
                }

            } else if( eName.equalsIgnoreCase("arg") ) {

                // Validate the parent element
                if( !elementStack.peek().equalsIgnoreCase("process") ) {

                    throw new SAXException("misplaced 'arg' tag [must be "
                                           +"directly inside of a 'process' "
                                           +"tag]");
                }

                String valueStr = attrs.getValue("value");

                if(valueStr == null) {
                    throw new SAXException
                                  ("'arg' tag is missing 'value' attribute");
                }
                currentProcessConfig.args.add(substituteValues(valueStr));

            } else if( eName.equalsIgnoreCase("autostart") ) {

                // Validate the parent element
                if( !elementStack.peek().equalsIgnoreCase("boot") ) {
                    throw new SAXException("misplaced 'autostart' tag");
                }

                String name = attrs.getValue("tag");

                if(name == null) {
                    throw new SAXException
                                ("'autostart' tag is missing 'tag' attribute");
                }
                bootProcesses.add(name);

            } else {
                throw new SAXException("unknown element [name='"+eName+"']");
            }

            // Update the element stack
            elementStack.push(eName);
        }

        public void endElement(String uri, String localName, String qName)
                        throws SAXException
        {
            String eName = localName;
            if( eName.equals("") ) eName = qName;

            // Test for and remove the top element in the stack
            if( elementStack.isEmpty() ||
                ( !eName.equalsIgnoreCase(elementStack.pop())) )
            {
                throw new SAXException("unexpected end element");
            }

            if( eName.equalsIgnoreCase("process") ) {
                // Add the new process to the process list
                processMap.put(currentProcessConfig.tag, currentProcessConfig);
                this.currentProcessConfig = null;
                configStack.pop();
            }
        }

        protected String substituteValues(String input) {
            return configStack.peek().substitutePropValues(input);
        }
    }
}
