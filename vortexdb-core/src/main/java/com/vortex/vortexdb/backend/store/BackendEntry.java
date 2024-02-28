
package com.vortex.vortexdb.backend.store;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.common.iterator.WrappedIterator;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.Idfiable;
import com.vortex.common.util.Bytes;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.StringEncoding;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public interface BackendEntry extends Idfiable {

    public static class BackendColumn implements Comparable<BackendColumn> {

        public byte[] name;
        public byte[] value;

        public static BackendColumn of(byte[] name, byte[] value) {
            BackendColumn col = new BackendColumn();
            col.name = name;
            col.value = value;
            return col;
        }

        @Override
        public String toString() {
            return String.format("%s=%s",
                                 StringEncoding.decode(name),
                                 StringEncoding.decode(value));
        }

        @Override
        public int compareTo(BackendColumn other) {
            if (other == null) {
                return 1;
            }
            return Bytes.compare(this.name, other.name);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof BackendColumn)) {
                return false;
            }
            BackendColumn other = (BackendColumn) obj;
            return Bytes.equals(this.name, other.name) &&
                   Bytes.equals(this.value, other.value);
        }
    }

    public VortexType type();

    @Override
    public Id id();

    public Id originId();

    public Id subId();

    public long ttl();

    public int columnsSize();
    public Collection<BackendColumn> columns();

    public void columns(Collection<BackendColumn> columns);
    public void columns(BackendColumn... columns);

    public void merge(BackendEntry other);
    public boolean mergable(BackendEntry other);

    public void clear();

    public default boolean belongToMe(BackendColumn column) {
        return Bytes.prefixWith(column.name, id().asBytes());
    }

    public default boolean olap() {
        return false;
    }

    public interface BackendIterator<T> extends Iterator<T>, AutoCloseable {

        public void close();

        public byte[] position();
    }

    public interface BackendColumnIterator
           extends BackendIterator<BackendColumn> {

        public static BackendColumnIterator empty() {
            return EMPTY;
        }

        public final BackendColumnIterator EMPTY = new BackendColumnIterator() {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public BackendColumn next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {
                // pass
            }

            @Override
            public byte[] position() {
                return null;
            }
        };
    }

    public static class BackendColumnIteratorWrapper
                  implements BackendColumnIterator {

        private final Iterator<BackendColumn> iter;

        public BackendColumnIteratorWrapper(BackendColumn... cols) {
            this.iter = Arrays.asList(cols).iterator();
        }

        public BackendColumnIteratorWrapper(Iterator<BackendColumn> cols) {
            E.checkNotNull(cols, "cols");
            this.iter = cols;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public BackendColumn next() {
            return iter.next();
        }

        @Override
        public void close() {
            WrappedIterator.close(this.iter);
        }

        @Override
        public byte[] position() {
            return null;
        }
    }
}
