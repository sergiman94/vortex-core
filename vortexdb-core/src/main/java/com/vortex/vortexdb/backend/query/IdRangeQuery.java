
package com.vortex.vortexdb.backend.query;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.Bytes;
import com.vortex.common.util.E;

public final class IdRangeQuery extends Query {

    private final Id start;
    private final Id end;
    private final boolean inclusiveStart;
    private final boolean inclusiveEnd;

    public IdRangeQuery(VortexType resultType, Id start, Id end) {
        this(resultType, null, start, end);
    }

    public IdRangeQuery(VortexType resultType, Query originQuery,
                        Id start, Id end) {
        this(resultType, originQuery, start, true, end, false);
    }

    public IdRangeQuery(Query originQuery,
                        Id start, boolean inclusiveStart,
                        Id end, boolean inclusiveEnd) {
        this(originQuery.resultType(), originQuery,
             start, inclusiveStart, end, inclusiveEnd);
    }

    public IdRangeQuery(VortexType resultType, Query originQuery,
                        Id start, boolean inclusiveStart,
                        Id end, boolean inclusiveEnd) {
        super(resultType, originQuery);
        E.checkArgumentNotNull(start, "The start parameter can't be null");
        this.start = start;
        this.end = end;
        this.inclusiveStart = inclusiveStart;
        this.inclusiveEnd = inclusiveEnd;
        if (originQuery != null) {
            this.copyBasic(originQuery);
        }
    }

    public Id start() {
        return this.start;
    }

    public Id end() {
        return this.end;
    }

    public boolean inclusiveStart() {
        return this.inclusiveStart;
    }

    public boolean inclusiveEnd() {
        return this.inclusiveEnd;
    }

    @Override
    public boolean empty() {
        return false;
    }

    @Override
    public boolean test(VortexElement element) {
        int cmp1 = Bytes.compare(element.id().asBytes(), this.start.asBytes());
        int cmp2 = Bytes.compare(element.id().asBytes(), this.end.asBytes());
        return (this.inclusiveStart ? cmp1 >= 0 : cmp1 > 0) &&
               (this.inclusiveEnd ? cmp2 <= 0 : cmp2 < 0);
    }

    @Override
    public IdRangeQuery copy() {
        return (IdRangeQuery) super.copy();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        assert sb.length() > 0;
        sb.deleteCharAt(sb.length() - 1); // Remove the last "`"
        sb.append(" id in range ")
          .append(this.inclusiveStart ? "[" : "(")
          .append(this.start)
          .append(", ")
          .append(this.end)
          .append(this.inclusiveEnd ? "]" : ")")
          .append("`");
        return sb.toString();
    }
}
