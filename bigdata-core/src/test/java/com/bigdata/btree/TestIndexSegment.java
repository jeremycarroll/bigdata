/*
 * Created by IntelliJ IDEA.
 * User: gossard
 * Date: Sep 23, 2010
 * Time: 2:40:27 PM
 */
package com.bigdata.btree;

import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import junit.framework.TestCase;

import java.io.File;
import java.util.UUID;

/**
 * Basic unit tests for IndexSegment class.
 */
public class TestIndexSegment extends TestCase {
    File outputDirectory;
    File outputFile;

    BTree sampleTree;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        outputFile = new File(getClass().getName() + ".seg").getAbsoluteFile();
        outputDirectory = outputFile.getParentFile();

        if ( outputFile.exists() && !outputFile.delete() )
            throw new RuntimeException("Could not delete test file -" + outputFile.getAbsolutePath());

        //just to be sure.
        outputFile.deleteOnExit();



        sampleTree = BTree.create(new SimpleMemoryRawStore(),
                new IndexMetadata(UUID.randomUUID())
        );

        for (int i = 0;i < 10;i++)
            sampleTree.insert("key-"+i, "value-"+i);

    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        outputFile.delete();
    }

    public void test_verify_getResourceMetadata() throws Exception {
        //write segment file from sample tree to disk.
        IndexSegmentCheckpoint checkpoint = IndexSegmentBuilder
                .newInstance(outputFile, outputDirectory, sampleTree.getEntryCount(),
                        sampleTree.rangeIterator(), 3, sampleTree.getIndexMetadata(),
                        System.currentTimeMillis() , true/* compactingMerge */, true /*bufferNodes*/)
                .call();

        //load the segment file from disk.
        IndexSegmentStore segStore = new IndexSegmentStore(outputFile);
        IndexSegment seg = segStore.loadIndexSegment();

        IResourceMetadata[] metaList = seg.getResourceMetadata();
        assertNotNull("cannot return null",metaList);
        assertEquals("must return only one item", 1, metaList.length);
        assertNotNull("item cannot be null",metaList[0]);
        assertTrue("resource metadata for IndexSegment must be instanceof SegmentMetadata", (metaList[0] instanceof SegmentMetadata) );

        SegmentMetadata meta = (SegmentMetadata)metaList[0];
        assertTrue("index segment metadata must return true for isIndexSegment()", meta.isIndexSegment() );
        assertFalse("index segment metadata must return false for isJournal()", meta.isJournal() );

        //expect short filename like 'foo.seg' , not absolute path.
        assertEquals("index metadata backing filename wasn't same as original?", outputFile.getName() , meta.getFile() );
    }
}