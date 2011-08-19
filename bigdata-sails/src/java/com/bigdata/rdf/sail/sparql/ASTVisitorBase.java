/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import org.openrdf.query.parser.sparql.ast.ASTAnd;
import org.openrdf.query.parser.sparql.ast.ASTAskQuery;
import org.openrdf.query.parser.sparql.ast.ASTAvg;
import org.openrdf.query.parser.sparql.ast.ASTBNodeFunc;
import org.openrdf.query.parser.sparql.ast.ASTBaseDecl;
import org.openrdf.query.parser.sparql.ast.ASTBasicGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTBind;
import org.openrdf.query.parser.sparql.ast.ASTBlankNode;
import org.openrdf.query.parser.sparql.ast.ASTBlankNodePropertyList;
import org.openrdf.query.parser.sparql.ast.ASTBound;
import org.openrdf.query.parser.sparql.ast.ASTCoalesce;
import org.openrdf.query.parser.sparql.ast.ASTCollection;
import org.openrdf.query.parser.sparql.ast.ASTCompare;
import org.openrdf.query.parser.sparql.ast.ASTConstraint;
import org.openrdf.query.parser.sparql.ast.ASTConstruct;
import org.openrdf.query.parser.sparql.ast.ASTConstructQuery;
import org.openrdf.query.parser.sparql.ast.ASTCount;
import org.openrdf.query.parser.sparql.ast.ASTDatasetClause;
import org.openrdf.query.parser.sparql.ast.ASTDatatype;
import org.openrdf.query.parser.sparql.ast.ASTDescribe;
import org.openrdf.query.parser.sparql.ast.ASTDescribeQuery;
import org.openrdf.query.parser.sparql.ast.ASTExistsFunc;
import org.openrdf.query.parser.sparql.ast.ASTFalse;
import org.openrdf.query.parser.sparql.ast.ASTFunctionCall;
import org.openrdf.query.parser.sparql.ast.ASTGraphGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTGraphPatternGroup;
import org.openrdf.query.parser.sparql.ast.ASTGroupClause;
import org.openrdf.query.parser.sparql.ast.ASTGroupConcat;
import org.openrdf.query.parser.sparql.ast.ASTGroupCondition;
import org.openrdf.query.parser.sparql.ast.ASTHavingClause;
import org.openrdf.query.parser.sparql.ast.ASTIRI;
import org.openrdf.query.parser.sparql.ast.ASTIRIFunc;
import org.openrdf.query.parser.sparql.ast.ASTIf;
import org.openrdf.query.parser.sparql.ast.ASTIn;
import org.openrdf.query.parser.sparql.ast.ASTIsBlank;
import org.openrdf.query.parser.sparql.ast.ASTIsIRI;
import org.openrdf.query.parser.sparql.ast.ASTIsLiteral;
import org.openrdf.query.parser.sparql.ast.ASTIsNumeric;
import org.openrdf.query.parser.sparql.ast.ASTLang;
import org.openrdf.query.parser.sparql.ast.ASTLangMatches;
import org.openrdf.query.parser.sparql.ast.ASTLimit;
import org.openrdf.query.parser.sparql.ast.ASTMath;
import org.openrdf.query.parser.sparql.ast.ASTMax;
import org.openrdf.query.parser.sparql.ast.ASTMin;
import org.openrdf.query.parser.sparql.ast.ASTMinusGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTNot;
import org.openrdf.query.parser.sparql.ast.ASTNotExistsFunc;
import org.openrdf.query.parser.sparql.ast.ASTNotIn;
import org.openrdf.query.parser.sparql.ast.ASTNumericLiteral;
import org.openrdf.query.parser.sparql.ast.ASTObjectList;
import org.openrdf.query.parser.sparql.ast.ASTOffset;
import org.openrdf.query.parser.sparql.ast.ASTOptionalGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTOr;
import org.openrdf.query.parser.sparql.ast.ASTOrderClause;
import org.openrdf.query.parser.sparql.ast.ASTOrderCondition;
import org.openrdf.query.parser.sparql.ast.ASTPathAlternative;
import org.openrdf.query.parser.sparql.ast.ASTPathElt;
import org.openrdf.query.parser.sparql.ast.ASTPathMod;
import org.openrdf.query.parser.sparql.ast.ASTPathOneInPropertySet;
import org.openrdf.query.parser.sparql.ast.ASTPathSequence;
import org.openrdf.query.parser.sparql.ast.ASTPrefixDecl;
import org.openrdf.query.parser.sparql.ast.ASTProjectionElem;
import org.openrdf.query.parser.sparql.ast.ASTPropertyList;
import org.openrdf.query.parser.sparql.ast.ASTPropertyListPath;
import org.openrdf.query.parser.sparql.ast.ASTQName;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTRDFLiteral;
import org.openrdf.query.parser.sparql.ast.ASTRegexExpression;
import org.openrdf.query.parser.sparql.ast.ASTSameTerm;
import org.openrdf.query.parser.sparql.ast.ASTSample;
import org.openrdf.query.parser.sparql.ast.ASTSelect;
import org.openrdf.query.parser.sparql.ast.ASTSelectQuery;
import org.openrdf.query.parser.sparql.ast.ASTStr;
import org.openrdf.query.parser.sparql.ast.ASTStrDt;
import org.openrdf.query.parser.sparql.ast.ASTStrLang;
import org.openrdf.query.parser.sparql.ast.ASTString;
import org.openrdf.query.parser.sparql.ast.ASTSum;
import org.openrdf.query.parser.sparql.ast.ASTTriplesSameSubject;
import org.openrdf.query.parser.sparql.ast.ASTTriplesSameSubjectPath;
import org.openrdf.query.parser.sparql.ast.ASTTrue;
import org.openrdf.query.parser.sparql.ast.ASTUnionGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.ASTWhereClause;
import org.openrdf.query.parser.sparql.ast.SimpleNode;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilderVisitor;
import org.openrdf.query.parser.sparql.ast.VisitorException;

/**
 * Base class for visitors of the SPARQL AST.
 * 
 * @author arjohn
 */
abstract class ASTVisitorBase implements SyntaxTreeBuilderVisitor {

    public Object visit(ASTAnd node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTAskQuery node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTAvg node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTBaseDecl node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTBasicGraphPattern node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTBind node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTBlankNode node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTBlankNodePropertyList node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTBNodeFunc node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTBound node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTCoalesce node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTCollection node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTCompare node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTConstraint node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTConstruct node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTConstructQuery node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTCount node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTDatasetClause node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTDatatype node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTDescribe node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTDescribeQuery node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTExistsFunc node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTFalse node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTFunctionCall node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTGraphGraphPattern node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTGraphPatternGroup node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTGroupClause node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTGroupConcat node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTGroupCondition node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTHavingClause node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTIf node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTIn node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTIRI node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTIRIFunc node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTIsBlank node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTIsIRI node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTIsLiteral node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTIsNumeric node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTLang node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTLangMatches node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTLimit node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTMath node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTMax node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTMin node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTMinusGraphPattern node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTNot node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTNotExistsFunc node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTNotIn node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTNumericLiteral node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTObjectList node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTOffset node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTOptionalGraphPattern node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTOr node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTOrderClause node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTOrderCondition node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTPathAlternative node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTPathElt node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTPathMod node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }
    
    public Object visit(ASTPathOneInPropertySet node, Object data)
    throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTPathSequence node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTPrefixDecl node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTProjectionElem node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTPropertyList node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTPropertyListPath node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }


    public Object visit(ASTQName node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTQueryContainer node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTRDFLiteral node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTRegexExpression node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTSameTerm node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTSample node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTSelect node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTSelectQuery node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTStr node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTStrDt node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTString node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTStrLang node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTSum node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTTriplesSameSubject node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTTriplesSameSubjectPath node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTTrue node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTUnionGraphPattern node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTVar node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(ASTWhereClause node, Object data)
            throws VisitorException {
        return node.childrenAccept(this, data);
    }

    public Object visit(SimpleNode node, Object data) throws VisitorException {
        return node.childrenAccept(this, data);
    }

}
