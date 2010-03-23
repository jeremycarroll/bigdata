package com.bigdata.rdf.sail;

import java.io.IOException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailRepository extends SailRepository {
   
    public BigdataSailRepository(BigdataSail sail) {
    
        super(sail);
        
    }
    
    public AbstractTripleStore getDatabase() {
        
        return ((BigdataSail) getSail()).getDatabase();
        
    }
    
    private BigdataSail getBigdataSail() {
        
        return (BigdataSail) getSail();
        
    }

    @Override
    public SailRepositoryConnection getConnection() throws RepositoryException {
        
        try {
        
            return new BigdataSailRepositoryConnection(this, 
                getSail().getConnection());
            
        } catch (SailException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }
    
    /**
     * Obtain a read-only connection to the database at the last commit point.
     * This connection should be used for all pure-readers, as the connection
     * will not be blocked by concurrent writers.
     * 
     * @return a read-only connection to the database
     */
    public SailRepositoryConnection getReadOnlyConnection() 
        throws RepositoryException {
        
        return new BigdataSailRepositoryConnection(this, 
            getBigdataSail().getReadOnlyConnection());
    }
    
    /**
     * Obtain a read-only connection to the database from a historical commit 
     * point. This connection should be used for all pure-readers, as the 
     * connection will not be blocked by concurrent writers.
     * 
     * @return a read-only connection to the database
     */
    public SailRepositoryConnection getReadOnlyConnection(long timestamp) 
        throws RepositoryException {
        
        return new BigdataSailRepositoryConnection(this, 
            getBigdataSail().getReadOnlyConnection(timestamp));
        
    }
    
    public SailRepositoryConnection getReadWriteConnection() 
        throws RepositoryException {
        
        try {
            
            return new BigdataSailRepositoryConnection(this, 
                getBigdataSail().getReadWriteConnection());
            
        } catch (IOException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }
    
    public SailRepositoryConnection getUnisolatedConnection() 
        throws RepositoryException {
        
        try {
            
            return new BigdataSailRepositoryConnection(this, 
                getBigdataSail().getUnisolatedConnection());
            
        } catch (InterruptedException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }

}
