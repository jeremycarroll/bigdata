/*
 * Created by IntelliJ IDEA.
 * User: gossard
 * Date: Aug 18, 2010
 * Time: 3:48:54 PM
 */
package com.bigdata;

import org.junit.Assert;

public class TestDataFinder extends Assert {


    public void testFindsFilesInTestDeployDirectory(){
        String deployResource = "testing/data/datafinder.txt";
        assertTrue("Didn't find the file in the deployment directory.",
                DataFinder.defaultFinder().exists( deployResource ));
    }
}
