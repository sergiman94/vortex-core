package com.vortex.client.structure.graph;

import com.vortex.client.driver.GraphManager;
import com.vortex.client.structure.GraphElement;
import com.vortex.common.util.E;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class GraphIterator<T extends GraphElement> implements Iterator<T> {

    private final GraphManager graphManager;
    private final int sizePerPage;
    private final Function<String, Pageable<T>> pageFetcher;
    private List<T> results;
    private String page;
    private int cursor;
    private boolean finished;

    public GraphIterator(final GraphManager graphManager, final int sizePerPage,
                         final Function<String, Pageable<T>> pageFetcher) {
        E.checkNotNull(graphManager, "Graph manager");
        E.checkNotNull(pageFetcher, "Page fetcher");
        this.graphManager = graphManager;
        this.sizePerPage = sizePerPage;
        this.pageFetcher = pageFetcher;
        this.results = null;
        this.page = "";
        this.cursor = 0;
        this.finished = false;
    }

    @Override
    public boolean hasNext() {
        if (this.results == null || this.cursor >= this.results.size()) {
            this.fetch();
        }
        assert this.results != null;
        return this.cursor < this.results.size();
    }

    private void fetch() {
        if (this.finished) {
            return;
        }
        Pageable<T> pageable = this.pageFetcher.apply(this.page);
        this.results = pageable.results();
        this.page = pageable.page();
        this.cursor = 0;
        E.checkState(this.results.size() <= this.sizePerPage,
                     "Server returned unexpected results: %s > %s",
                     this.results.size(), this.sizePerPage);
        if (this.results.size() < this.sizePerPage || this.page == null) {
            this.finished = true;
        }
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        T elem = this.results.get(this.cursor++);
        E.checkState(elem != null,
                     "The server data is invalid, some records are null");
        elem.attachManager(this.graphManager);
        return elem;
    }
}
