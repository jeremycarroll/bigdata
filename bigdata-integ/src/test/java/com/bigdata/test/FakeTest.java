/***********************************************************************
 *
 * $Id$
 *
 * Copyright (c) 2000-2010 Nokia Corporation.
 *
 * This material, including documentation and any related computer
 * programs, is protected by copyright controlled by Nokia Corporation.
 * All rights are reserved.  Copying, including reproducing, storing,
 * adapting or translating, any or all of this material requires the prior
 * written consent of Nokia Corporation.  This material also contains
 * confidential information which may not be disclosed to others without
 * the prior written consent of Nokia Corporation.
 **********************************************************************/
package com.bigdata.test;


import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author blevine
 *
 */
public class FakeTest
{

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }
    
    @Test
    public void theTest()
    {
        System.out.println("HELLO WORLD!!!!");
    }

}
