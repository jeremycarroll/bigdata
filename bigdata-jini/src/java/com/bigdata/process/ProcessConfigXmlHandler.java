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
package com.bigdata.process;

import static com.bigdata.process.Constants.*;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

class ProcessConfigXmlHandler extends DefaultHandler {

    private final Map<String, ProcessManagement> processMap;
    private final Map<ProcessInfo, RoleInfo> roleMap;
    private final Stack<String> elementStack = new Stack<String>();
    private final List<Set<String>> restartGroups;

    private String type;
    private String layout;
    private String process;

    private ArrayList<ProcessInfo> pInfoList;

    ProcessConfigXmlHandler(Map<String,      ProcessManagement> processMap,
                            Map<ProcessInfo, RoleInfo>          roleMap)
    {
        this.processMap = processMap;
        this.roleMap = roleMap;
        this.restartGroups = new ArrayList<Set<String>>();
    }

    public List<Set<String>> getRestartGroups() {
        return restartGroups;
    }

    @Override
    public void startDocument() throws SAXException {
        if( !elementStack.empty() ) {
            throw new SAXException("unexpected start of document");
        }
        elementStack.push("ROOT");
    }

    @Override
    public void endDocument() throws SAXException {
        if( (elementStack.empty()) || (!elementStack.pop().equals("ROOT")) ) {
            throw new SAXException("Unexpected end of document");
        }
    }

    @Override
    public void startElement(String     uri,
                             String     localName,
                             String     qName,
                             Attributes attrs) throws SAXException
    {
        if( elementStack.isEmpty() ) {
            throw new SAXException("unexpected element placement");
        }

        String eName = localName;
        if( eName.equals("") ) eName = qName;

        if( eName.equalsIgnoreCase("definitions") ) {

            // Validate the parent element
            if( !elementStack.peek().equals("ROOT") ) {
                throw new SAXException("misplaced element - <definitions>");
            }
            // No attributes

        } else if( eName.equalsIgnoreCase("types") ) {// <types>

            // Validate the parent element
            if( !elementStack.peek().equals("definitions") ) {
                throw new SAXException("misplaced element - <types>");
            }
            // No attributes

        } else if( eName.equalsIgnoreCase("type") ) {// <type>

            // Validate the parent element
            if( !elementStack.peek().equals("types") ) {
                throw new SAXException("misplaced element - <type>");
            }

            // Save the name of this type
            type = attrs.getValue("name");
            if(type == null) {
                throw new SAXException("missing <type> attribute ['name']");
            }

        } else if( eName.equalsIgnoreCase("layouts") ) {// <layouts>

            // Validate the parent element
            if( !elementStack.peek().equals("type") ) {
                throw new SAXException("misplaced element - <layouts>");
            }
            // No attributes

        } else if( eName.equalsIgnoreCase("layout") ) {// <layout>

            // Validate the parent element
            if( !elementStack.peek().equals("layouts") ) {
                throw new SAXException("misplaced element - <layout>");
            }

            // Save the name of this layout
            layout = attrs.getValue("name");
            if(layout == null) {
                throw new SAXException("missing <layout> attribute ['name']");
            }

        } else if( eName.equalsIgnoreCase("roles") ) {// <roles>

            // Validate the parent element
            if( !elementStack.peek().equals("layout") ) {
                throw new SAXException("misplaced element - <roles>");
            }
            // No attributes

        } else if( eName.equalsIgnoreCase("role") ) {// <role>

            // Validate the parent element
             if( !elementStack.peek().equals("roles") ) {
                throw new SAXException("misplaced element - <role>");
            }

            // Save the name of this role
            String role = attrs.getValue("name");
            if(role == null) {
                throw new SAXException("missing <role> attribute ['name']");
            }

            // Populate the roleMap with the appropriate ProcessInfo to
            // RoleInfo mappings, taking wildcards into account
            pInfoList = new ArrayList<ProcessInfo>();
            ProcessInfo pInfo = null;
            if("*".equals(type)) {
                for(int i=0; i<NODE_TYPES.length; i++) {
                    if("*".equals(layout)) {
                        for(int j=0; j<NODE_LAYOUTS.length; j++) {
                            if("*".equals(role)) {
                                for(int k=0; k<NODE_ROLES.length; k++) {
                                    pInfo = new ProcessInfo(NODE_TYPES[i],
                                                            NODE_LAYOUTS[j],
                                                            NODE_ROLES[k]);
                                    RoleInfo rInfo = new RoleInfo(pInfo);
                                    roleMap.put(pInfo, rInfo);
                                    pInfoList.add(pInfo);
                                }
                            } else {//role not wildcarded
                                pInfo = new ProcessInfo(NODE_TYPES[i],
                                                        NODE_LAYOUTS[j], role);
                                RoleInfo rInfo = new RoleInfo(pInfo);
                                roleMap.put(pInfo, rInfo);
                                pInfoList.add(pInfo);
                            }
                        }
                    } else {//layout not wildcarded
                        if("*".equals(role)) {
                            for(int k=0; k<NODE_ROLES.length; k++) {
                                pInfo = new ProcessInfo(NODE_TYPES[i],
                                                        layout,
                                                        NODE_ROLES[k]);
                                RoleInfo rInfo = new RoleInfo(pInfo);
                                roleMap.put(pInfo, rInfo);
                                pInfoList.add(pInfo);
                            }
                        } else {//role not wildcarded
                            pInfo = new ProcessInfo(NODE_TYPES[i],layout,role);
                            RoleInfo rInfo = new RoleInfo(pInfo);
                            roleMap.put(pInfo, rInfo);
                            pInfoList.add(pInfo);
                        }
                    }
                }
            } else {//type not wildcarded
                if("*".equals(layout)) {
                    for(int j=0; j<NODE_LAYOUTS.length; j++) {
                        if("*".equals(role)) {
                            for(int k=0; k<NODE_ROLES.length; k++) {
                                pInfo = new ProcessInfo(type,
                                                        NODE_LAYOUTS[j],
                                                        NODE_ROLES[k]);
                                RoleInfo rInfo = new RoleInfo(pInfo);
                                roleMap.put(pInfo, rInfo);
                                pInfoList.add(pInfo);
                            }
                        } else {//role not wildcarded
                            pInfo = new ProcessInfo(type,NODE_LAYOUTS[j],role);
                            RoleInfo rInfo = new RoleInfo(pInfo);
                            roleMap.put(pInfo, rInfo);
                            pInfoList.add(pInfo);
                        }
                    }
                } else {//layout not wildcarded
                    if("*".equals(role)) {
                        for(int k=0; k<NODE_ROLES.length; k++) {
                            pInfo = new ProcessInfo(type,layout,NODE_ROLES[k]);
                            RoleInfo rInfo = new RoleInfo(pInfo);
                            roleMap.put(pInfo, rInfo);
                            pInfoList.add(pInfo);
                        }
                    } else {//role not wildcarded
                        pInfo = new ProcessInfo(type, layout, role);
                        RoleInfo rInfo = new RoleInfo(pInfo);
                        roleMap.put(pInfo, rInfo);
                        pInfoList.add(pInfo);
                    }
                }
            }

        } else if( eName.equalsIgnoreCase("start") ) {// <start>

            // Validate the parent element
            if( !elementStack.peek().equals("role") ) {
                throw new SAXException("misplaced element - <start>");
            }

            String tagString = attrs.getValue("tag");
            if(tagString == null) {
                throw new SAXException("missing <start> attribute - ['tag']");
            }

            // Add the process tag to the role
            for(ProcessInfo pInfo : pInfoList) {
                RoleInfo rInfo = roleMap.get(pInfo);
                rInfo.addProcess(tagString);
            }
        } else if( eName.equalsIgnoreCase("process") ) {// <process>

            // Validate the parent element
            if( !elementStack.peek().equals("definitions") ) {
                throw new SAXException("misplaced element - <process>");
            }

            // Save the name of this process
            process = attrs.getValue("tag");
            if(process == null) {
                throw new SAXException("missing <process> attribute ['tag']");
            }

            ProcessManagement pMgmt = new ProcessStateMachine(process);
            processMap.put(pMgmt.getTag(), pMgmt);

        } else if( eName.equalsIgnoreCase("autorestart") ) {// <autorestart>

            // Validate the parent element
            if( !elementStack.peek().equals("process") ) {
                throw new SAXException("misplaced element - <autorestart>");
            }

            String countString = attrs.getValue("count");
            String intervalString = attrs.getValue("interval");

            // Set the configuration in the process
            ProcessManagement pMgmt = processMap.get(process);

            int count = 3;// # of restart attempts to allow
            try {
                count = (countString != null) ? 
                               Integer.parseInt(countString) : 3;
            } catch(NumberFormatException e) {
                throw new SAXException("invalid value for 'count' tag "
                                       +"["+countString+"]");
            }

            int interval = 120;// # of seconds between restart attempts
            try {
                interval = (intervalString != null) ? 
                               Integer.parseInt(intervalString) : 120;
            } catch(NumberFormatException e) {
                throw new SAXException("invalid value for 'interval' tag "
                                       +"["+intervalString+"]");
            }
            pMgmt.setAutoRestartRate(count, interval);

        } else if(eName.equalsIgnoreCase("start-dependency")) {

            // Validate the parent element
            if( !elementStack.peek().equals("process") ) {
                throw new SAXException
                              ("misplaced element - <start-dependency>");
            }

            String tagString = attrs.getValue("tag");
            if(tagString == null) {
                throw new SAXException("missing attribute - ['tag']");
            }

            // Set the configuration in the process
            ProcessManagement pMgmt = processMap.get(process);
            pMgmt.addStartDependency(tagString);

        } else if( eName.equalsIgnoreCase("restart-group") ) {//<restart-group>

            // Validate the parent element
            if( !elementStack.peek().equals("definitions") ) {
                throw new SAXException("misplaced element - <restart-group>");
            }

            restartGroups.add(new TreeSet<String>());

        } else if( eName.equalsIgnoreCase("member") ) {

            // Validate the parent element
            if( !elementStack.peek().equals("restart-group") ) {
                throw new SAXException("misplaced element - <member>");
            }

            String tagString = attrs.getValue("tag");
            if(tagString == null) {
                throw new SAXException("missing attribute - ['tag']");
            }

            // Add the tag to the restart group
            Set<String> group = restartGroups.get(restartGroups.size()-1);
            group.add(tagString);

        } else {
            throw new SAXException("unknown element - <"+eName+">");
        }

        // Update the element stack
        elementStack.push(eName);
    }

    @Override
    public void endElement(String uri, String localName, String qName)
                    throws SAXException
    {
        String eName = localName;
        if( eName.equals("") ) eName = qName;

        // Test for and remove the top element in the stack
        if( elementStack.isEmpty() || 
            ( !eName.equalsIgnoreCase(elementStack.pop()) ) )
        {
            throw new SAXException("unmatched element at end of stack");
        }
    }
}
