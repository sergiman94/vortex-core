
package com.vortex.vortexdb.backend.page;

import com.vortex.common.util.CollectionUtil;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.page.IdHolder.FixedIdHolder;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.common.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SortByCountIdHolderList extends IdHolderList {

    private static final long serialVersionUID = -7779357582250824558L;

    private final List<IdHolder> mergedHolders;

    public SortByCountIdHolderList(boolean paging) {
        super(paging);
        this.mergedHolders = new ArrayList<>();
    }

    @Override
    public boolean add(IdHolder holder) {
        if (this.paging()) {
            return super.add(holder);
        }
        this.mergedHolders.add(holder);

        if (super.isEmpty()) {
            Query parent = holder.query().originQuery();
            super.add(new SortByCountIdHolder(parent));
        }
        SortByCountIdHolder sortHolder = (SortByCountIdHolder) this.get(0);
        sortHolder.merge(holder);
        return true;
    }

    private class SortByCountIdHolder extends FixedIdHolder {

        private final Map<Id, Integer> ids;

        public SortByCountIdHolder(Query parent) {
            super(new MergedQuery(parent), ImmutableSet.of());
            this.ids = InsertionOrderUtil.newMap();
        }

        public void merge(IdHolder holder) {
            for (Id id : holder.all()) {
                this.ids.compute(id, (k, v) -> v == null ? 1 : v + 1);
                Query.checkForceCapacity(this.ids.size());
            }
        }

        @Override
        public boolean keepOrder() {
            return true;
        }

        @Override
        public Set<Id> all() {
            return CollectionUtil.sortByValue(this.ids, false).keySet();
        }

        @Override
        public String toString() {
            return String.format("%s{merged:%s}",
                                 this.getClass().getSimpleName(), this.query);
        }
    }

    private class MergedQuery extends Query {

        public MergedQuery(Query parent) {
            super(parent.resultType(), parent);
        }

        @Override
        public String toString() {
            return SortByCountIdHolderList.this.mergedHolders.toString();
        }
    }
}
