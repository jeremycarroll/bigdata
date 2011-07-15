package com.bigdata.rdf.sparql.ast;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;

/**
 * An optional or non-optional collection of query nodes that run together in
 * a group.
 */
public class JoinGroupNode extends GroupNodeBase 
		implements IGroupNode, IQueryNode {

	/**
	 * Construct a non-optional join group.
	 */
	public JoinGroupNode() {

		super(false);
		
	}

	public JoinGroupNode(final boolean optional) {

		super(optional);
		
	}

	/**
	 * Calculate the variable bindings that will definitely be produced by this
	 * join group. A group will produce bindings for vars from its statement
	 * patterns. It may produce bindings for vars from its subgroups, but those
	 * are not counted since they are either optional or a union of different
	 * possible groups and thus we cannot be sure what if anything they will
	 * actually bind.
	 */
	private Set<IVariable<?>> getProducedBindings() {
		
		final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
		
		for (IQueryNode child : this) {
			
			if (child instanceof StatementPatternNode) {
				
				final StatementPatternNode sp = (StatementPatternNode) child;
				
				vars.addAll(sp.getProducedBindings());
				
			}
			
		}
		
		return vars;
		
	}

	/**
	 * Calculate the set of variables that are known to be bound coming into
	 * this group (i.e. variables bound by statement patterns in ancestor
	 * join groups). Useful for determining when to run various filters. 
	 */
	public Set<IVariable<?>> getIncomingBindings() {
		
		final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
		
		IGroupNode parent = getParent();
		
		while (parent != null) {
			
			if (parent instanceof JoinGroupNode) {
				
				final JoinGroupNode joinGroup = (JoinGroupNode) parent;
				
				vars.addAll(joinGroup.getProducedBindings());
				
			}
			
			parent = parent.getParent();
			
		}
		
		return vars;
		
	}
	
	/**
	 * Return only the statement pattern child nodes in this group.
	 */
	public List<StatementPatternNode> getStatementPatterns() {
		
		final List<StatementPatternNode> spNodes = 
			new LinkedList<StatementPatternNode>();
		
		for (IQueryNode node : this) {
			
			if (node instanceof StatementPatternNode) {
				
				spNodes.add((StatementPatternNode) node);
				
			}
			
		}
		
		return spNodes;
		
	}

	/**
	 * Return only the filter child nodes in this group.
	 */
	public Collection<FilterNode> getFilters() {
		
		final Collection<FilterNode> filters = new LinkedList<FilterNode>();
		
		for (IQueryNode node : this) {
			
			if (node instanceof FilterNode) {
				
				filters.add((FilterNode) node);
				
			}
			
		}
		
		return filters;
		
	}

	/**
	 * Return only the filter child nodes in this group that will be fully
	 * bound before running any of the joins in this group.
	 */
	public Collection<FilterNode> getPreFilters() {
		
		/*
		 * Get the variables known to be bound starting out.
		 */
		final Set<IVariable<?>> knownBound = getIncomingBindings();

		/*
		 * Get the filters that are bound by this set of known bound vars.
		 */
		final Collection<FilterNode> filters = getBoundFilters(knownBound);
		
		return filters;
		
	}

	/**
	 * Return only the filter child nodes in this group that will be fully
	 * bound only by running the joins in this group.
	 */
	public Collection<FilterNode> getJoinFilters() {
		
		/*
		 * Get the variables known to be bound starting out.
		 */
		final Set<IVariable<?>> knownBound = getIncomingBindings();
		
		/*
		 * Add all the variables bound by statement patterns in this group.
		 */
		knownBound.addAll(getProducedBindings());
		
		/*
		 * Get the filters that are bound by this set of known bound vars.
		 */
		final Collection<FilterNode> filters = getBoundFilters(knownBound);
		
		/*
		 * Remove the preConditional filters (those fully bound by just
		 * incoming bindings).
		 */
		filters.removeAll(getPreFilters());
		
		return filters;
		
	}
	
	/**
	 * Return only the filter child nodes in this group that will not be fully
	 * bound even after running the joins in this group.  We must assume they
	 * will become fully bound by running the sub-groups (unions and optionals).
	 */
	public Collection<FilterNode> getPostFilters() {

		/*
		 * Start with all the filters in this group.
		 */
		final Collection<FilterNode> filters = getFilters();
		
		/*
		 * Get the variables known to be bound starting out.
		 */
		final Set<IVariable<?>> knownBound = getIncomingBindings();
		
		/*
		 * Add all the variables bound by statement patterns in this group.
		 */
		knownBound.addAll(getProducedBindings());
		
		/*
		 * Get the filters that are bound by this set of known bound vars.
		 */
		final Collection<FilterNode> preAndJoinFilters = getBoundFilters(knownBound);
		
		/*
		 * Remove the preFilters and joinFilters, leaving only
		 * the postFilters.
		 */
		filters.removeAll(preAndJoinFilters);

		return filters;
		
	}

	/**
	 * Helper method to determine the set of filters that will be fully bound
	 * assuming the specified set of variables is bound.
	 */
	private final Collection<FilterNode> getBoundFilters(
			final Set<IVariable<?>> knownBound) {
		
		final Collection<FilterNode> filters = 
			new LinkedList<FilterNode>();
		
		for (IQueryNode node : this) {
			
			if (!(node instanceof FilterNode))
				continue;
		
			final FilterNode filter = (FilterNode) node;
			
			final Set<IVariable<?>> filterVars = filter.getConsumedVars();
			
			boolean allBound = true;
			
			for (IVariable<?> v : filterVars) {
				
				allBound &= knownBound.contains(v);
				
			}
			
			if (allBound) {
				
				filters.add(filter);
				
			}
			
		}

		return filters;

	}
	
	/**
	 * A "simple optional" is an optional sub-group that contains only one
	 * statement pattern, no sub-groups of its own, and no filters that
	 * require materialized variables. We can lift these "simple optionals"
	 * into the parent group without incurring the costs of launching
	 * a {@link SubqueryOp}.
	 */
	public boolean isSimpleOptional() {
		
		// first, the whole group must be optional
		if (!isOptional()) {
			return false;
		}
		
		/*
		 * Second, make sure we have only one statement pattern, no sub-queries,
		 * and no filters that require materialization.
		 */
		StatementPatternNode sp = null;
		
		for (IQueryNode node : this) {
			
			if (node instanceof IGroupNode) {
				
				return false;
				
			} else if (node instanceof StatementPatternNode) {
			
				// already got one
				if (sp != null) {
					return false;
				}
				
				sp = (StatementPatternNode) node;
				
			} else if (node instanceof FilterNode) {
				
				final FilterNode filter = (FilterNode) node;
				
				if (filter.getMaterializationRequirement() != 
						INeedsMaterialization.Requirement.NEVER) {
				
					return false;
					
				}
			}
			
		}
		
		// if we've made it this far, we are simple optional 
		return true;
		
	}
	
	/**
	 * Get the single "simple optional" statement pattern.
	 * <p>
	 * See {@link #isSimpleOptional()}.
	 */
	public StatementPatternNode getSimpleOptional() {
		
		if (!isSimpleOptional()) {
			throw new RuntimeException("not a simple optional join group");
		}
		
		for (IQueryNode node : this) {
			
			if (node instanceof StatementPatternNode) {
				
				return (StatementPatternNode) node;
				
			}
			
		}
		
		throw new RuntimeException("not a simple optional join group");
		
	}
	
	public String toString() {
		
		return toString(0);
		
	}
	
	public String toString(final int indent) {
		
		final String _indent;
		if (indent <= 0) {
			
			_indent = "";
			
		} else {
			
			final StringBuilder sb = new StringBuilder();
			for (int i = 0; i < indent; i++) {
				sb.append(" ");
			}
			_indent = sb.toString();
			
		}
		
		final StringBuilder sb = new StringBuilder();
		
		if (isOptional()) {
			sb.append(_indent).append("optional\n");
		}
		sb.append(_indent).append("{\n");
		
		for (IQueryNode n : this) {
			if (!(n instanceof StatementPatternNode)) {
				continue;
			}
			sb.append(n.toString(indent+2)).append("\n");
		}
		
		for (IQueryNode n : this) {
			if (!(n instanceof FilterNode)) {
				continue;
			}
			sb.append(n.toString(indent+2)).append("\n");
		}
		
		for (IQueryNode n : this) {
			if (!(n instanceof UnionNode)) {
				continue;
			}
			sb.append(((UnionNode)n).toString(indent+2)).append("\n");
		}
		
		for (IQueryNode n : this) {
			if (!(n instanceof JoinGroupNode)) {
				continue;
			}
			sb.append(((JoinGroupNode)n).toString(indent+2)).append("\n");
		}
		
		sb.append(_indent).append("}");
		
		return sb.toString();
		
	}

}
