
package com.vortex.vortexdb.backend.page;

import com.vortex.vortexdb.backend.id.Id;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public final class PageIds {

    public static final PageIds EMPTY = new PageIds(ImmutableSet.of(),
                                                    PageState.EMPTY);

    private final Set<Id> ids;
    private final PageState pageState;

    public PageIds(Set<Id> ids, PageState pageState) {
        this.ids = ids;
        this.pageState = pageState;
    }

    public Set<Id> ids() {
        return this.ids;
    }

    public String page() {
        if (this.pageState == null) {
            return null;
        }
        return this.pageState.toString();
    }

    public PageState pageState() {
        return this.pageState;
    }

    public boolean empty() {
        return this.ids.isEmpty();
    }
}
