
package com.vortex.vortexdb.backend.serializer;

import com.vortex.vortexdb.backend.page.PageState;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.backend.store.BackendEntry.BackendIterator;
import com.vortex.vortexdb.backend.store.BackendEntryIterator;
import com.vortex.common.util.E;

import java.util.function.BiFunction;

public class BinaryEntryIterator<Elem> extends BackendEntryIterator {

    protected final BackendIterator<Elem> results;
    protected final BiFunction<BackendEntry, Elem, BackendEntry> merger;

    protected BackendEntry next;

    public BinaryEntryIterator(BackendIterator<Elem> results, Query query,
                               BiFunction<BackendEntry, Elem, BackendEntry> m) {
        super(query);

        E.checkNotNull(results, "results");
        E.checkNotNull(m, "merger");

        this.results = results;
        this.merger = m;
        this.next = null;

        if (query.paging()) {
            assert query.offset() == 0L;
            assert PageState.fromString(query.page()).offset() == 0;
            this.skipPageOffset(query.page());
        } else {
            this.skipOffset();
        }
    }

    @Override
    public void close() throws Exception {
        this.results.close();
    }

    @Override
    protected final boolean fetch() {
        assert this.current == null;
        if (this.next != null) {
            this.current = this.next;
            this.next = null;
        }

        while (this.results.hasNext()) {
            Elem elem = this.results.next();
            BackendEntry merged = this.merger.apply(this.current, elem);
            E.checkState(merged != null, "Error when merging entry");
            if (this.current == null) {
                // The first time to read
                this.current = merged;
            } else if (merged == this.current) {
                // The next entry belongs to the current entry
                assert this.current != null;
                if (this.sizeOf(this.current) >= INLINE_BATCH_SIZE) {
                    break;
                }
            } else {
                // New entry
                assert this.next == null;
                this.next = merged;
                break;
            }

            // When limit exceed, stop fetching
            if (this.reachLimit(this.fetched() - 1)) {
                // Need remove last one because fetched limit + 1 records
                this.removeLastRecord();
                this.results.close();
                break;
            }
        }

        return this.current != null;
    }

    @Override
    protected final long sizeOf(BackendEntry entry) {
        return sizeOfEntry(entry);
    }

    @Override
    protected final long skip(BackendEntry entry, long skip) {
        BinaryBackendEntry e = (BinaryBackendEntry) entry;
        E.checkState(e.columnsSize() > skip, "Invalid entry to skip");
        for (long i = 0; i < skip; i++) {
            e.removeColumn(0);
        }
        return e.columnsSize();
    }

    @Override
    protected PageState pageState() {
        byte[] position = this.results.position();
        if (position == null) {
            position = PageState.EMPTY_BYTES;
        }
        return new PageState(position, 0, (int) this.count());
    }

    private void removeLastRecord() {
        int lastOne = this.current.columnsSize() - 1;
        ((BinaryBackendEntry) this.current).removeColumn(lastOne);
    }

    public final static long sizeOfEntry(BackendEntry entry) {
        /*
         * 3 cases:
         *  1) one vertex per entry
         *  2) one edge per column (one entry <==> a vertex),
         *  3) one element id per column (one entry <==> an index)
         */
        if (entry.type().isEdge() || entry.type().isIndex()) {
            return entry.columnsSize();
        }
        return 1L;
    }
}
