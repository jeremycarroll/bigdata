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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

/**
 * Similar to the standard JUnit Parameterized runner but uses a ConditionalRunner rather than the default
 * runner when generating the parameterized test proxies.
 * 
 * @author blevine
 *
 */
public class ConditionalParameterized extends Suite
{

    private final ArrayList<Runner> runners = new ArrayList<Runner>();
    
    /**
     * Constructor
     * 
     * @param klass
     * @throws Throwable
     */
    public ConditionalParameterized(Class<?> klass) throws Throwable
    {
        super(klass, Collections.<Runner> emptyList());
        List<Object[]> parametersList = getParametersList(getTestClass());
        
        for (int i = 0; i < parametersList.size(); i++)
        {
            runners.add(new TestClassRunnerForParameters(getTestClass().getJavaClass(), parametersList, i));
        }
    }

    @Override
    protected List<Runner> getChildren()
    {
        return runners;
    }

    @SuppressWarnings("unchecked")
    private List<Object[]> getParametersList(TestClass klass) throws Throwable
    {
        return (List<Object[]>) getParametersMethod(klass).invokeExplosively(null);
    }

    private FrameworkMethod getParametersMethod(TestClass testClass) throws Exception
    {
        List<FrameworkMethod> methods = testClass.getAnnotatedMethods(Parameters.class);
        for (FrameworkMethod each : methods)
        {
            int modifiers = each.getMethod().getModifiers();
            if (Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers))
                return each;
        }

        throw new Exception("No public static parameters method on class " + testClass.getName());
    }
    

    private class TestClassRunnerForParameters extends ConditionalRunner
    {
        private final int parmSetNumber;
        private final List<Object[]> parmList;

        TestClassRunnerForParameters(Class<?> type, List<Object[]> parameterList, int i) throws InitializationError
        {
            super(type);
            parmList = parameterList;
            parmSetNumber = i;
        }

        @Override
        public Object createTest() throws Exception
        {
            return getTestClass().getOnlyConstructor().newInstance(computeParams());
        }

        private Object[] computeParams() throws Exception
        {
            try
            {
                return parmList.get(parmSetNumber);
            }
            catch (ClassCastException e)
            {
                throw new Exception(String.format("%s.%s() must return a Collection of arrays.", getTestClass().getName(), getParametersMethod(getTestClass()).getName()));
            }
        }

        @Override
        protected String getName()
        {
            return String.format("[%s]", parmSetNumber);
        }

        @Override
        protected String testName(final FrameworkMethod method)
        {
            return String.format("%s[%s]", method.getName(), parmSetNumber);
        }

        @Override
        protected void validateConstructor(List<Throwable> errors)
        {
            validateOnlyOneConstructor(errors);
        }

        @Override
        protected Statement classBlock(RunNotifier notifier)
        {
            return childrenInvoker(notifier);
        }
    }

}
