
package com.vortex.vortexdb.job.computer;

import com.vortex.vortexdb.job.Job;

import java.util.Map;

public interface Computer {

    public String name();

    public String category();

    public Object call(Job<Object> job, Map<String, Object> parameters);

    public void checkParameters(Map<String, Object> parameters);
}
