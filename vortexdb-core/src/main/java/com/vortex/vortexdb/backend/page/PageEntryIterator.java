
package com.vortex.vortexdb.backend.page;

import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.vortexdb.exception.NotSupportException;
import com.vortex.common.iterator.CIter;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.NoSuchElementException;

public class PageEntryIterator<R> implements CIter<R> {

    private final QueryList<R> queries;
    private final long pageSize;
    private final PageInfo pageInfo;
    private final QueryResults<R> queryResults; // for upper layer

    private QueryList.PageResults<R> pageResults;
    private long remaining;

    public PageEntryIterator(QueryList<R> queries, long pageSize) {
        this.queries = queries;
        this.pageSize = pageSize;
        this.pageInfo = this.parsePageInfo();
        this.queryResults = new QueryResults<>(this, queries.parent());

        this.pageResults = QueryList.PageResults.emptyIterator();
        this.remaining = queries.parent().limit();
    }

    private PageInfo parsePageInfo() {
        String page = this.queries.parent().pageWithoutCheck();
        PageInfo pageInfo = PageInfo.fromString(page);
//        E.checkState(pageInfo.offset() < this.queries.total(),
//                     "Invalid page '%s' with an offset '%s' exceeds " +
//                     "the size of IdHolderList", page, pageInfo.offset());
        return pageInfo;
    }

    @Override
    public boolean hasNext() {
        if (this.pageResults.get().hasNext()) {
            return true;
        }
        return this.fetch();
    }

    private boolean fetch() {
        if ((this.remaining != Query.NO_LIMIT && this.remaining <= 0L) ||
            this.pageInfo.offset() >= this.queries.total()) {
            return false;
        }

        long pageSize = this.pageSize;
        if (this.remaining != Query.NO_LIMIT && this.remaining < pageSize) {
            pageSize = this.remaining;
        }
        this.closePageResults();
        this.pageResults = this.queries.fetchNext(this.pageInfo, pageSize);
        assert this.pageResults != null;
        this.queryResults.setQuery(this.pageResults.query());

        if (this.pageResults.get().hasNext()) {
            if (!this.pageResults.hasNextPage()) {
                this.pageInfo.increase();
            } else {
                this.pageInfo.page(this.pageResults.page());
            }
            this.remaining -= this.pageResults.total();
            return true;
        } else {
            this.pageInfo.increase();
            return this.fetch();
        }
    }

    private void closePageResults() {
        if (this.pageResults != QueryList.PageResults.EMPTY) {
            CloseableIterator.closeIterator(this.pageResults.get());
        }
    }

    @Override
    public R next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        return this.pageResults.get().next();
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if (PageInfo.PAGE.equals(meta)) {
            if (this.pageInfo.offset() >= this.queries.total()) {
                return null;
            }
            return this.pageInfo;
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }

    @Override
    public void close() throws Exception {
        this.closePageResults();
    }

    public QueryResults<R> results() {
        return this.queryResults;
    }
}
