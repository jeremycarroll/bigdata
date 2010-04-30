package com.bigdata.rdf.lexicon;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.search.Hiterator;
import com.bigdata.search.IHit;

public interface ITextIndexer {
	public void index(int capacity,Iterator<BigdataValue> valuesIterator);
	public void create();
	public void destroy();
	public Hiterator search(final String languageCode, final String text) throws InterruptedException;
	public Hiterator search(final String query, final String languageCode,final boolean prefixMatch);
	public Hiterator search(final String query, final String languageCode, final double minCosine, final int maxRank);
	public Hiterator search(final String query, final String languageCode, final boolean prefixMatch, final double minCosine, final int maxRank, long timeout, final TimeUnit unit);
}
