
package com.vortex.vortexdb.backend.query;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.Bytes;
import com.vortex.common.util.E;

public final class IdPrefixQuery extends Query {

    private final Id start;
    private final boolean inclusiveStart;
    private final Id prefix;

    public IdPrefixQuery(VortexType resultType, Id prefix) {
        this(resultType, null, prefix, true, prefix);
    }

    public IdPrefixQuery(Query originQuery, Id prefix) {
        this(originQuery.resultType(), originQuery, prefix, true, prefix);
    }

    public IdPrefixQuery(Query originQuery, Id start, Id prefix) {
        this(originQuery.resultType(), originQuery, start, true, prefix);
    }

    public IdPrefixQuery(Query originQuery,
                         Id start, boolean inclusive, Id prefix) {
        this(originQuery.resultType(), originQuery, start, inclusive, prefix);
    }

    public IdPrefixQuery(VortexType resultType, Query originQuery,
                         Id start, boolean inclusive, Id prefix) {
        super(resultType, originQuery);
        E.checkArgumentNotNull(start, "The start parameter can't be null");
        this.start = start;
        this.inclusiveStart = inclusive;
        this.prefix = prefix;
        if (originQuery != null) {
            this.copyBasic(originQuery);
        }
    }

    public Id start() {
        return this.start;
    }

    public boolean inclusiveStart() {
        return this.inclusiveStart;
    }

    public Id prefix() {
        return this.prefix;
    }

    @Override
    public boolean empty() {
        return false;
    }

    @Override
    public boolean test(VortexElement element) {
        byte[] elem = element.id().asBytes();
        int cmp = Bytes.compare(elem, this.start.asBytes());
        boolean matchedStart = this.inclusiveStart ? cmp >= 0 : cmp > 0;
        boolean matchedPrefix = Bytes.prefixWith(elem, this.prefix.asBytes());
        return matchedStart && matchedPrefix;
    }

    @Override
    public IdPrefixQuery copy() {
        return (IdPrefixQuery) super.copy();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        assert sb.length() > 0;
        sb.deleteCharAt(sb.length() - 1); // Remove the last "`"
        sb.append(" id prefix with ").append(this.prefix);
        if (this.start != this.prefix) {
            sb.append(" and start with ").append(this.start)
              .append("(")
              .append(this.inclusiveStart ? "inclusive" : "exclusive")
              .append(")");
        }
        sb.append("`");
        return sb.toString();
    }
}
