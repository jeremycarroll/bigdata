package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.IIndex;

/**
 * Register a named index (unisolated write operation).
 * <p>
 * Note: The return value of {@link #doTask()} is the {@link UUID} of the named
 * index. You can test this value to determine whether the index was created
 * based on the supplied {@link IIndex} object or whether the index was
 * pre-existing at the time that this operation was executed.
 * <p>
 * Note: the registered index will NOT be visible to unisolated readers or
 * isolated operations until the next commit. However, unisolated writers that
 * execute after the index has been registered will be able to see the
 * registered index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RegisterIndexTask extends AbstractTask {

    final private IIndex btree;
    
    /**
     * @param journal
     * @param name
     * @param ndx
     *            The index object. Use
     * 
     * <pre>
     * UUID indexUUID = UUID.randomUUID();
     * 
     * new UnisolatedBTree(journal, indexUUID)
     * </pre>
     * 
     * to register a new index that supports isolation.
     */
    public RegisterIndexTask(ConcurrentJournal journal, String name, IIndex ndx) {

        super(journal, ITx.UNISOLATED, false/*readOnly*/, name);
        
        if(ndx==null) throw new NullPointerException();
        
        this.btree = ndx;
        
    }

    /**
     * Create the named index if it does not exist.
     * 
     * @return The {@link UUID} of the named index.
     */
    protected Object doTask() throws Exception {

        journal.assertOpen();

        String name = getOnlyResource();
        
        synchronized (journal.name2Addr) {

            try {
                
                // add to the persistent name map.
                journal.name2Addr.add(name, btree);

                log.info("Registered index: name=" + name + ", class="
                        + btree.getClass() + ", indexUUID="
                        + btree.getIndexUUID());
                
            } catch(IndexExistsException ex) {
                
                IIndex ndx = journal.name2Addr.get(name);
                
                UUID indexUUID = ndx.getIndexUUID();
                
                log.info("Index exists: name="+name+", indexUUID="+indexUUID);
                
                return indexUUID;
                
            }

        }

        // report event (the application has access to the named index).
        ResourceManager.openUnisolatedBTree(name);

        return btree.getIndexUUID();

    }

}