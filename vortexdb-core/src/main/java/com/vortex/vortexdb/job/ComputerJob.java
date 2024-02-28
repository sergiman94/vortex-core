
package com.vortex.vortexdb.job;

import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.vortexdb.job.computer.Computer;
import com.vortex.vortexdb.job.computer.ComputerPool;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;

import java.util.Map;

public class ComputerJob extends SysJob<Object> {

    public static final String COMPUTER = "computer";

    public static boolean check(String name, Map<String, Object> parameters) {
        Computer computer = ComputerPool.instance().find(name);
        if (computer == null) {
            return false;
        }
        computer.checkParameters(parameters);
        return true;
    }

    public String computerConfigPath() {
        return this.params().configuration().get(CoreOptions.COMPUTER_CONFIG);
    }

    @Override
    public String type() {
        return COMPUTER;
    }

    @Override
    public Object execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);

        Object value = map.get("computer");
        E.checkArgument(value instanceof String,
                        "Invalid computer name '%s'", value);
        String name = (String) value;

        value = map.get("parameters");
        E.checkArgument(value instanceof Map,
                        "Invalid computer parameters '%s'", value);
        @SuppressWarnings("unchecked")
        Map<String, Object> parameters = (Map<String, Object>) value;

        ComputerPool pool = ComputerPool.instance();
        Computer computer = pool.find(name);
        E.checkArgument(computer != null,
                        "There is no computer method named '%s'", name);
        return computer.call(this, parameters);
    }
}
