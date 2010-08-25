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

package com.bigdata.util;

import com.bigdata.util.config.LogUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import java.lang.reflect.Method;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.activation.ActivationException;
import java.rmi.activation.ActivationID;

import java.security.SecureClassLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.StringTokenizer;

import net.jini.io.MarshalledInstance;
import net.jini.loader.ClassAnnotation;

/** 
 * This class provides useful utilities for creating and manipulating
 * class loaders. Although it can be used for other purposes, it is
 * generally intended for debugging.
 */
public class ClassLoaderUtil {

    /** Configure logger */
    private static Logger logger = 
                              LogUtil.getLog4jLogger(ClassLoaderUtil.class);

    // Private constructor to prevent instantiation
    private ClassLoaderUtil() { }

    /**
     * Utility method that converts the components of a <code>String</code>
     * representing a classpath into file <code>URL</code>(s).
     *
     * @param classpath <code>String</code> containing components separated
     *                  by path separators that represent the components
     *                  making up a classpath
     *
     * @return a <code>URL[]</code> where 
     *         each element of the array corresponds to one of the components
     *         in the <code>classpath</code> parameter. The path components
     *         are (potentially) expanded via 
     *         <code>File.getCanonicalFile()</code> before converting to a
     *         <code>URL</code> format.
     *                       
     * @throws java.net.MalformedURLException 
     *         If the path cannot be parsed as a URL
     * @throws java.net.IOException 
     *         If an I/O error occurs, 
     *         which is possible because the construction of 
     *         the canonical pathname may require filesystem queries 
     */
    public static URL[] getClasspathURLs(String classpath)
     throws IOException, MalformedURLException
    {
        StringTokenizer st = new StringTokenizer(classpath,File.pathSeparator);
        URL[] urls = new URL[st.countTokens()];
        for (int i=0; st.hasMoreTokens(); i++) {
            urls[i] = 
                new File(st.nextToken()).getCanonicalFile().toURI().toURL();
        }
        return urls;
    }

    /**
     * Utility method that converts the components of a <code>String</code>
     * representing a codebase into standard <code>URL</code>(s).
     *
     * @param codebase  <code>String</code> containing components separated
     *                  by spaces in which each component is in 
     *                  <code>URL</code> format.
     *
     * @return a <code>URL[]</code> where
     *         each element of the array corresponds to one of the components
     *         in the <code>codebase</code> parameter
     *                       
     * @throws java.net.MalformedURLException 
     */
    public static URL[] getCodebaseURLs(String codebase)
        throws MalformedURLException
    {
        StringTokenizer st = new StringTokenizer(codebase);
        URL[] urls = new URL[st.countTokens()];
        for (int i=0; st.hasMoreTokens(); i++) {
            urls[i] = new URL(st.nextToken());
        }
        return urls;
    }
    
    /**
     * Utility method that converts the components of a <code>String</code>
     * representing a codebase or classpath into <code>URL</code>(s).
     *
     * @param importCodebase <code>String</code> assumed (in order) to be
     *                       either
     *            1) a space delimited set of <code>URL</code>(s)
     *            representing a codebase or
     *            2) a <code>File.pathSeparator</code> delimited set 
     *            of class paths.
     *
     * @return a <code>URL[]</code> where 
     *         each element of the array corresponds to one of the components
     *         in the <code>importCodebase</code> parameter
     *                       
     * @throws java.net.MalformedURLException 
     *         If the path cannot be parsed as a URL
     * @throws java.net.IOException 
     *         If an I/O error occurs, 
     *         which is possible because the construction of 
     *         the canonical pathname may require filesystem queries 
     */
    public static URL[] getImportCodebaseURLs(String importCodebase)
     throws IOException, MalformedURLException
    {
        try {
            return getCodebaseURLs(importCodebase);
	} catch (MalformedURLException me) {
            return getClasspathURLs(importCodebase);
        }
    }
    
    /**
     * Utility method that retrieves the components making up the class loader
     * delegation tree for the current context class loader and returns each
     * in an <code>ArrayList</code>.
     *
     * @return an <code>ArrayList</code> instance in which each element of the
     *         list is one of the components making up the current delegation
     *         tree.
     */
    private static ArrayList getContextClassLoaderTree() {
        Thread curThread = Thread.currentThread();
        ClassLoader curClassLoader = curThread.getContextClassLoader();
        return getClassLoaderTree(curClassLoader);
    }

    /**
     * Utility method that retrieves the components making up the class loader
     * delegation tree for the given <code>classloader</code> parameter and
     * returns them via an <code>ArrayList</code>.
     *
     * @param classloader <code>ClassLoader</code> instance whose delegation
     *                    tree is to be retrieved and returned
     *
     * @return an <code>ArrayList</code> instance in which each element of the
     *         list is one of the components making up the delegation tree
     *         of the given class loader.
     */
    private static ArrayList getClassLoaderTree(ClassLoader classloader) {
        ArrayList loaderList = new ArrayList();
        while(classloader != null) {
            loaderList.add(classloader);
            classloader = classloader.getParent();
        }
        loaderList.add(null); //Append boot classloader
        Collections.reverse(loaderList);
        return loaderList;
    }

    /**
     * Utility method that displays the class loader delegation tree for
     * the current context class loader. For each class loader in the tree,
     * this method displays the locations from which that class loader
     * will retrieve and load requested classes.
     * <p>
     * This method can be useful when debugging problems related to the
     * receipt of exceptions such as <code>ClassNotFoundException</code>.
     */
    public static void displayContextClassLoaderTree() {
        Thread curThread = Thread.currentThread();
        ClassLoader curClassLoader = curThread.getContextClassLoader();
        displayClassLoaderTree(curClassLoader);
    }

    /**
     * Utility method that displays the class loader delegation tree for
     * the given class loader. For each class loader in the tree, this
     * method displays the locations from which that class loader will
     * retrieve and load requested classes.
     * <p>
     * This method can be useful when debugging problems related to the
     * receipt of exceptions such as <code>ClassNotFoundException</code>.
     *
     * Note that although this class' logger level is used to determine 
     * whether or not to display any information at all, the output is
     * actually displayed using System.out.println. This is done to
     * produce more readable output than the logger might produce.
     *
     * @param description descriptive <code>String</code> that, if 
     *                    non-<code>null</code>, will be logged prior to
     *                    displaying the information about the
     *                    <code>classloader</code>.
     *
     * @param classloader <code>ClassLoader</code> instance whose delegation
     *                    tree is to be displayed.
     */
    public static void displayClassLoaderTree(ClassLoader classloader) {
        displayClassLoaderTree(null, classloader);
    }

    public static void displayClassLoaderTree(String      description,
                                              ClassLoader classloader)
    {
        if( logger.isEnabledFor(Level.DEBUG) ) {
            if(description != null) {
                logger.log(Level.DEBUG, description);
            }

            ArrayList loaderList = getClassLoaderTree(classloader);
            System.out.println("ClassLoader Tree has " 
	        + loaderList.size() + " levels");
            System.out.println("  cl0 -- Boot ClassLoader ");
            ClassLoader curClassLoader = null;
            for(int i=1; i < loaderList.size(); i++) {
                System.out.println("   |");
                curClassLoader = (ClassLoader)loaderList.get(i);
                System.out.print("  cl"+i+" -- ClassLoader "
                                       +curClassLoader+": ");
                if(curClassLoader instanceof URLClassLoader) {
                    URL[] urls = ((URLClassLoader)(curClassLoader)).getURLs();
                    if(urls != null) {
                        System.out.print(urls[0]);
                        for(int j=1;j<urls.length;j++){
                            System.out.print(", "+urls[j]);
                        }
                    } else {//urls == null
                        System.out.print("null search path");
                    }
                } else {
                    if(curClassLoader instanceof SecureClassLoader) {
                        System.out.print("is instance of SecureClassLoader");
                    } else {
                        System.out.print("is unknown ClassLoader type");
                    }
                }
                System.out.println("");
            }
            System.out.println("");
        }
    }

    /**
     * Handles a <i>class loader mismatch</i> between the given
     * <code>Serializable</code> object and the given <code>Class</code>
     * type.
     *
     * If the class name of the given <code>obj</code> parameter
     * is the same as the name of <code>classType</code>, but 
     * <code>obj</code> is not an instance of <code>classType</code>,
     * then the difference may be due to unequal class loaders for the
     * two parameters; which is referred to as a <i>class loader mismatch</i>.
     * When such a mismatch occurs, the <code>instanceof</code> operator
     * will return <code>false</code> and attempts to cast the given
     * <code>obj</code> to the given <code>classType</code> will result
     * in a <code>ClassCastException</code>.
     *
     * To address the situation just described, this method attempts to
     * "reload" the given <code>obj</code>, using the <code>ClassLoader</code>
     * of <code>classType</code>. This is accomplished by first
     * marshalling and then unmarshalling the given <code>obj</code>,
     * while the <i>current context class loader</i> is set to the
     * <code>ClassLoader</code> of <code>classType</code>.
     *
     * Upon success, the newly loaded object is returned; which can then
     * be successfully cast to the given <code>classType</code>. If the
     * reload operation fails, <code>null</code> is returned.
     */
    public static Serializable instanceOf(Serializable obj, Class classType) {

        if( classType.isInstance(obj) ) return obj;

        Class  objClass         = obj.getClass();
        ClassLoader classTypeCl = classType.getClassLoader();
        //marshall-and-unmarshal using the class type's classloader
        try {
            MarshalledInstance mInst = new MarshalledInstance(obj);
            Serializable newObj = 
                   (Serializable)(mInst.get(classTypeCl, false, null, null));
            if( classType.isInstance(newObj) ) return newObj;

        } catch(Throwable t) {
            return null;
        }
        return null;
    }

}
