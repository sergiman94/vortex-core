package com.vortex.client.exception;

import java.util.Collection;

public class NotAllCreatedException extends ServerException {

    private static final long serialVersionUID = -8795820552805040556L;

    private Collection<?> ids;

    public NotAllCreatedException(String message, Collection<?> ids,
                                  Object... args) {
        super(message, args);
        this.ids = ids;
    }

    public Collection<?> ids() {
        return this.ids;
    }
}
