package com.bigdata.rdf.sail.changesets;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.StatementEnum;

public class ChangeRecord implements IChangeRecord {
    
    private final BigdataStatement stmt;
    
    private final ChangeAction action;
    
    private final StatementEnum oldType;
    
    public ChangeRecord(final BigdataStatement stmt, 
            final ChangeAction action) {
        
        this(stmt, action, null);
        
    }
    
    public ChangeRecord(final BigdataStatement stmt, final ChangeAction action, 
            final StatementEnum oldType) {
        
        this.stmt = stmt;
        this.action = action;
        this.oldType = oldType;
        
    }
    
    public ChangeAction getChangeAction() {
        
        return action;
        
    }

    public StatementEnum getOldStatementType() {
        
        return oldType;
        
    }

    public BigdataStatement getStatement() {
        
        return stmt;
        
    }
}
