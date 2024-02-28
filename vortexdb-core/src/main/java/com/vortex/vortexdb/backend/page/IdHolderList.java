
package com.vortex.vortexdb.backend.page;

import com.vortex.common.util.E;

import java.util.ArrayList;
import java.util.Collection;

public class IdHolderList extends ArrayList<IdHolder> {

    private static final IdHolderList EMPTY_P = new IdHolderList(true);
    private static final IdHolderList EMPTY_NP = new IdHolderList(false);

    private static final long serialVersionUID = -738694176552424990L;

    private final boolean paging;

    public static IdHolderList empty(boolean paging) {
        IdHolderList empty = paging ? EMPTY_P : EMPTY_NP;
        empty.clear();
        return empty;
    }

    public IdHolderList(boolean paging) {
        this.paging = paging;
    }

    public boolean paging() {
        return this.paging;
    }

    @Override
    public boolean add(IdHolder holder) {
        E.checkArgument(this.paging == holder.paging(),
                        "The IdHolder to be linked must be " +
                        "in same paging mode");
        return super.add(holder);
    }

    @Override
    public boolean addAll(Collection<? extends IdHolder> idHolders) {
        for (IdHolder idHolder : idHolders) {
            this.add(idHolder);
        }
        return true;
    }
}
