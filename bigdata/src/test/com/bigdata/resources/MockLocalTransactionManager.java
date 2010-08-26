package com.bigdata.resources;

import com.bigdata.journal.AbstractLocalTransactionManager;
//BTM import com.bigdata.journal.ITransactionService;
//BTM
import com.bigdata.journal.TransactionService;

/**
 * Mock implementation used by some of the unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class MockLocalTransactionManager extends AbstractLocalTransactionManager {

//BTM    private final ITransactionService txService;
private final TransactionService txService;
    
//BTM    public MockLocalTransactionManager(ITransactionService txService) {
public MockLocalTransactionManager(TransactionService txService) {

        this.txService = txService;

    }

//BTM    public ITransactionService getTransactionService() {
public TransactionService getTransactionService() {

        return txService;
        
    }

}