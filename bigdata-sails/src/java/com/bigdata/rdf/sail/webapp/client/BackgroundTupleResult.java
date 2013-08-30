/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2008-2009.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.webapp.client;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.resultio.QueryResultParseException;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.repository.sparql.query.QueueCursor;

/**
 * Provides concurrent access to tuple results as they are being parsed.
 * 
 * @author James Leigh
 */
public class BackgroundTupleResult extends TupleQueryResultImpl implements
		TupleQueryResult, Runnable, TupleQueryResultHandler {

	final private Object closeLock = new Object();

	private volatile boolean closed;

	private Thread parserThread;

	final private TupleQueryResultParser parser;

	final private InputStream in;

	final private HttpEntity entity;

	final private QueueCursor<BindingSet> queue;

	// No need to synchronize this field because visibility is guaranteed
	// by happens-before of CountDownLatch's countDown() and await()
	private List<String> bindingNames;

	final private CountDownLatch bindingNamesReady = new CountDownLatch(1);

    public BackgroundTupleResult(final TupleQueryResultParser parser,
            final InputStream in, final HttpEntity entity) {
        
		this(new QueueCursor<BindingSet>(10), parser, in, entity);
		
	}

    public BackgroundTupleResult(final QueueCursor<BindingSet> queue,
            final TupleQueryResultParser parser, final InputStream in,
            final HttpEntity entity) {
        super(Collections.<String>emptyList(), queue);
		this.queue = queue;
		this.parser = parser;
		this.in = in;
		this.entity = entity;
	}

    @Override
	public void close() throws QueryEvaluationException {
		synchronized (closeLock) {
			if (!closed) {
				closed = true;
				if (parserThread != null) {
					parserThread.interrupt();
				}
			}
		}
	}

    @Override
	public List<String> getBindingNames() {
		try {
            /*
             * Note: close() will interrupt the parserThread if it is running.
             * That will cause the latch to countDown() and unblock this method
             * if the binding names have not yet been parsed from the
             * connection.
             */
		    bindingNamesReady.await();
			queue.checkException();
			if (closed)
				throw new UndeclaredThrowableException(null, "Result closed");
			return bindingNames;
		} catch (InterruptedException e) {
			throw new UndeclaredThrowableException(e);
		} catch (QueryEvaluationException e) {
			throw new UndeclaredThrowableException(e);
		}
	}

    @Override
	public void run() {
		synchronized (closeLock) {
			if (closed) {
				// Result was closed before it was run.
				// Need to unblock latch
				bindingNamesReady.countDown();
				return;
			}
			parserThread = Thread.currentThread();
		}
		boolean completed = false;
		try {
			parser.setTupleQueryResultHandler(this);
			parser.parse(in);
			// release connection back into pool if all results have been read
			EntityUtils.consume(entity);
			completed = true;
		} catch (TupleQueryResultHandlerException e) {
			// parsing cancelled or interrupted
		} catch (QueryResultParseException e) {
			queue.toss(e);
		} catch (IOException e) {
			queue.toss(e);
		} finally {
			synchronized (closeLock) {
				parserThread = null;
			}
			queue.done();
			bindingNamesReady.countDown();
			if (!completed) {
				try {
					EntityUtils.consume(entity);
				} catch (IOException ex) { }
			}
		}
	}

	@Override
	public void startQueryResult(final List<String> bindingNames)
			throws TupleQueryResultHandlerException {
		this.bindingNames = bindingNames;
		bindingNamesReady.countDown();
	}

    @Override
	public void handleSolution(final BindingSet bindingSet)
			throws TupleQueryResultHandlerException {
		if (closed)
			throw new TupleQueryResultHandlerException("Result closed");
		try {
			queue.put(bindingSet);
		} catch (InterruptedException e) {
			throw new TupleQueryResultHandlerException(e);
		}
	}

    @Override
	public void endQueryResult() throws TupleQueryResultHandlerException {
		// no-op
	}

}
