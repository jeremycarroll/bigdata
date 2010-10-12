package com.bigdata.test;

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
 * Similar to the Parameterized runner but uses a ConditionalRunner rather than the default
 * runner when generating the parameterized test instances.
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
