

package com.vortex.common.perf;

import java.util.List;

public interface Stopwatch extends Cloneable {

    public Path id();
    public String name();
    public Path parent();

    public void startTime(long startTime);
    public void endTime(long startTime);

    public void lastStartTime(long startTime);

    public long times();
    public long totalTimes();
    public long totalChildrenTimes();

    public long totalCost();
    public void totalCost(long otherCost);

    public long minCost();
    public long maxCost();

    public long totalWasted();
    public long totalSelfWasted();
    public long totalChildrenWasted();

    public void fillChildrenTotal(List<Stopwatch> children);

    public Stopwatch copy();

    public Stopwatch child(String name);
    public Stopwatch child(String name, Stopwatch watch);

    public boolean empty();
    public void clear();

    public default String toJson() {
        int len = 200 + this.name().length() + this.parent().length();
        StringBuilder sb = new StringBuilder(len);
        sb.append("{");
        sb.append("\"parent\":\"").append(this.parent()).append("\"");
        sb.append(",\"name\":\"").append(this.name()).append("\"");
        sb.append(",\"times\":").append(this.times());
        sb.append(",\"total_cost\":").append(this.totalCost());
        sb.append(",\"min_cost\":").append(this.minCost());
        sb.append(",\"max_cost\":").append(this.maxCost());
        sb.append(",\"total_self_wasted\":").append(this.totalSelfWasted());
        sb.append(",\"total_children_wasted\":").append(
                                                 this.totalChildrenWasted());
        sb.append(",\"total_children_times\":").append(
                                                 this.totalChildrenTimes());
        sb.append("}");
        return sb.toString();
    }

    public static Path id(Path parent, String name) {
        if (parent == Path.EMPTY && name == Path.ROOT_NAME) {
            return Path.EMPTY;
        }
        return new Path(parent, name);
    }

    public static final class Path implements Comparable<Path> {

        public static final String ROOT_NAME = "root";
        public static final Path EMPTY = new Path("");

        private final String path;

        public Path(String self) {
            this.path = self;
        }

        public Path(Path parent, String name) {
            if (parent == EMPTY) {
                this.path = name;
            } else {
                int len = parent.length() + 1 + name.length();
                StringBuilder sb = new StringBuilder(len);
                sb.append(parent.path).append('/').append(name);

                this.path = sb.toString();
            }
        }

        public int length() {
            return this.path.length();
        }

        @Override
        public int hashCode() {
            return this.path.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this.hashCode() != obj.hashCode()) {
                return false;
            }
            if (!(obj instanceof Path)) {
                return false;
            }
            Path other = (Path) obj;
            return this.path.equals(other.path);
        }

        @Override
        public int compareTo(Path other) {
            return this.path.compareTo(other.path);
        }

        @Override
        public String toString() {
            return this.path;
        }

        public boolean endsWith(String name) {
            return this.path.endsWith(name);
        }
    }
}
