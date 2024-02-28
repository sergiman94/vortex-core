
package com.vortex.vortexdb.task;

import com.vortex.vortexdb.type.define.SerialEnum;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

public enum TaskStatus implements SerialEnum {

    UNKNOWN(0, "UNKNOWN"),

    NEW(1, "new"),
    SCHEDULING(2, "scheduling"),
    SCHEDULED(3, "scheduled"),
    QUEUED(4, "queued"),
    RESTORING(5, "restoring"),
    RUNNING(6, "running"),
    SUCCESS(7, "success"),
    CANCELLING(8, "cancelling"),
    CANCELLED(9, "cancelled"),
    FAILED(10, "failed");

    // NOTE: order is important(RESTORING > RUNNING > QUEUED) when restoring
    public static final List<TaskStatus> PENDING_STATUSES = ImmutableList.of(
           TaskStatus.RESTORING, TaskStatus.RUNNING, TaskStatus.QUEUED);

    public static final Set<TaskStatus> COMPLETED_STATUSES = ImmutableSet.of(
           TaskStatus.SUCCESS, TaskStatus.CANCELLED, TaskStatus.FAILED);

    private byte status = 0;
    private String name;

    static {
        SerialEnum.register(TaskStatus.class);
    }

    TaskStatus(int status, String name) {
        assert status < 256;
        this.status = (byte) status;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.status;
    }

    public String string() {
        return this.name;
    }
}
