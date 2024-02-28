
package com.vortex.vortexdb.analyzer;

import java.util.Set;

public interface Analyzer {

    public Set<String> segment(String text);
}
