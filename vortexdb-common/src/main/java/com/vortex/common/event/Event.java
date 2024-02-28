package com.vortex.common.event;

import com.vortex.common.util.E;

import java.util.Arrays;
import java.util.Collections;

public class Event extends java.util.EventObject{

    private static final long serialVersionUID = 1625973849208342813L;

    private String name;
    private Object[] args;

    public Event (Object source, String event) { this(source, event, Collections.emptyList().toArray());}

    public Event(Object source, String event, Object... args) {
        super(source);
        this.name = event;
        this.args = args;
    }

    public String getName() {
        return name;
    }

    public Object[] getArgs() {
        return args;
    }

    public void checkArgs(Class<?>... classes) throws IllegalArgumentException {
        E.checkArgument(this.args.length == classes.length, "The args count of event '%s' should be %s(actual%s)", this.name, classes.length, this.args.length);
    }

    @Override
    public String toString() {
        return String.format("Event{name='%s', args=%s}",
                this.name, Arrays.asList(this.args));
    }
}
