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
/*
 * Created on Jan 23, 2008
 */

package com.bigdata.search;

import com.bigdata.journal.AbstractIndexManagerTestCase;
import java.io.StringReader;
import java.util.Properties;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.ProxyTestCase;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Simple test verifies that the {@link FullTextIndex} data are restart safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Parameterized.class)
public class TestSearchRestartSafe extends ProxyTestCase<IIndexManager> {

    /**
     * 
     */
    public TestSearchRestartSafe(AbstractIndexManagerTestCase delegate) {
        setDelegate(delegate);
    }

    @Parameters
    public static Collection<Object[]> getDelegates() {
        return ProxyTestCase.getDelegateGroup2();
    };

//    final File file;
//    {
//        
//        try {
//
//            file = File.createTempFile(getName(), ".tmp");
//   
//            System.err.println("file="+file);
//         
//        } catch (IOException ex) {
//
//            throw new RuntimeException(ex);
//
//        }
//    }

//    public Properties getProperties() {
//
//        Properties properties = new Properties( super.getProperties() );
//            
//        // Note: overrides the buffer mode so that we can re-open it.
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
//                .toString());
//        
//        properties.setProperty(Options.FILE,file.toString());
//        
//        return properties;
//        
//    }
    
    @Test
    public void test_simple() throws InterruptedException {

        final Properties properties = getProperties();
        
        IIndexManager indexManager = getStore(properties);

        try {

            final String NAMESPACE = "test";

            /*
             * Index a document.
             */
            final long docId = 12L;
            final int fieldId = 3;
            final String text = "The quick brown dog";
            final String languageCode = "EN";
            {

                final FullTextIndex ndx = new FullTextIndex(indexManager,
                        NAMESPACE, ITx.UNISOLATED, properties);

                ndx.create();

                final TokenBuffer buffer = new TokenBuffer(2, ndx);

                ndx.index(buffer, docId, fieldId, languageCode,
                        new StringReader(text));

                ndx.index(buffer, docId + 1, fieldId, languageCode,
                        new StringReader("The slow brown cow"));

                buffer.flush();

            }

            /* Search w/o restart. */
            {

                final FullTextIndex ndx = new FullTextIndex(indexManager,
                        NAMESPACE, ITx.UNISOLATED, properties);

                final Hiterator<?> itr = ndx.search(text, languageCode);

                assertEquals(2, itr.size());

                assertTrue(itr.hasNext());

                final IHit hit1 = itr.next();

                System.err.println("hit1:" + hit1);

                /*
                 * Note: with cosine computation only the first hit is visited.
                 */

                assertFalse(itr.hasNext());

            }

            /*
             * Shutdown and restart.
             */
            indexManager = reopenStore(indexManager);

            /* Search with restart. */
            {

                final FullTextIndex ndx = new FullTextIndex(indexManager,
                        NAMESPACE, ITx.UNISOLATED, properties);

                final Hiterator<?> itr = ndx.search(text, languageCode);

                assertEquals(2, itr.size());

                assertTrue(itr.hasNext());

                final IHit hit1 = itr.next();

                System.err.println("hit1:" + hit1);

                /*
                 * Note: with cosine computation only the first hit is visited.
                 */

                assertFalse(itr.hasNext());

            }

        } finally {

            indexManager.destroy();

        }

    }

}
