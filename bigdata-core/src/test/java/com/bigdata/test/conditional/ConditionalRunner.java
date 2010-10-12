/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.test.conditional;

import java.lang.reflect.Method;

import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * A subclass of the default JUnit standard runner that checks for and processes the @IgnoreIf 
 * conditional annotation
 * 
 * @author blevine
 * 
 */
public class ConditionalRunner extends BlockJUnit4ClassRunner
{

    /**
     * Constructor 
     * 
     * @param klass
     * @throws InitializationError
     */
    public ConditionalRunner(Class<?> klass) throws InitializationError
    {
        super(klass);
    }

    /* (non-Javadoc)
     * @see org.junit.runners.BlockJUnit4ClassRunner#runChild(org.junit.runners.model.FrameworkMethod, org.junit.runner.notification.RunNotifier)
     */
    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier)
    {
        // Is the annotation on the method?
        IgnoreIf ann = method.getAnnotation(IgnoreIf.class);
        
        // Is the annotation on the class?
        if (ann == null)
        {
            ann = method.getMethod().getDeclaringClass().getAnnotation(IgnoreIf.class);
        }
        
        // If we found the annotation on the method or it's declaring class, check the condition
        // and if true, skip this method (test case).
        if (ann != null)
        {
            Class<?> conditionalClass = ann.value();
            
            if (conditionalClass != null)
            {
                String conditionalClassName = conditionalClass.getName();
                // Does the conditional class have a public static boolean test() method?  If so, invoke it.
                // If not, it's an error.
                try
                {
                    Method testMethod = conditionalClass.getMethod("test");
                    Class<?> returnType = testMethod.getReturnType();
                    
                    if ((returnType) == null || (returnType != Boolean.TYPE))
                    {
                        throw new IllegalArgumentException("@IgnoreIf conditional class, " + conditionalClassName + 
                                                           "  must have a static no arg 'test' method that returns boolean");                       
                    }
                    
                    Boolean result = (Boolean)testMethod.invoke(null);
                    
                    if (result)
                    {
                        EachTestNotifier eachNotifier = makeNotifier(method, notifier);
                        eachNotifier.fireTestIgnored();
                        return;
                    }
                }
                catch (Exception e)
                {
                    throw new IllegalArgumentException("@IgnoreIf conditional class, " + conditionalClassName + 
                                                       "  must have a static no arg 'test' method that returns boolean");
                }
            }
            else
            {
                throw new IllegalArgumentException("No class specified for @IgnoreIf annotation.");
            }
        }
        
        // If we didn't find the annotation or if the annotation's condition evaluated to false process
        // the test case normally.
        
        super.runChild(method, notifier);
    }

    /**
     * Unfortunately declared private in super-class. Cloned from there
     * 
     * @param method
     * @param notifier
     * @return
     */
    private EachTestNotifier makeNotifier(FrameworkMethod method, RunNotifier notifier)
    {
        Description description = describeChild(method);
        return new EachTestNotifier(notifier, description);
    }
}
