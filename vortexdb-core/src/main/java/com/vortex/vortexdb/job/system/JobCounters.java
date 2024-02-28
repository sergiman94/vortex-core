
package com.vortex.vortexdb.job.system;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.vortexdb.structure.VortexIndex;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class JobCounters {

    private ConcurrentHashMap<String, JobCounter> jobCounters =
                                                  new ConcurrentHashMap<>();

    public JobCounter jobCounter(Vortex g) {
        int batch = g.option(CoreOptions.TASK_TTL_DELETE_BATCH);
        String graph = g.name();
        if (!this.jobCounters.containsKey(graph)) {
            this.jobCounters.putIfAbsent(graph, new JobCounter(batch));
        }
        return this.jobCounters.get(graph);
    }

    public static class JobCounter {

        private AtomicInteger jobs;
        private Set<VortexElement> elements;
        private Set<VortexIndex> indexes;
        private int batchSize;

        public JobCounter(int batchSize) {
            this.jobs = new AtomicInteger(0);
            this.elements = ConcurrentHashMap.newKeySet();
            this.indexes = ConcurrentHashMap.newKeySet();
            this.batchSize = batchSize;
        }

        public int jobs() {
            return this.jobs.get();
        }

        public void decrement() {
            this.jobs.decrementAndGet();
        }

        public void increment() {
            this.jobs.incrementAndGet();
        }

        public Set<VortexElement> elements() {
            return this.elements;
        }

        public Set<VortexIndex> indexes() {
            return this.indexes;
        }

        public void clear(Object object) {
            if (object instanceof VortexElement) {
                this.elements = ConcurrentHashMap.newKeySet();
            } else {
                assert object instanceof VortexIndex;
                this.indexes = ConcurrentHashMap.newKeySet();
            }
        }

        public boolean addAndTriggerDelete(Object object) {
            return object instanceof VortexElement ?
                   addElementAndTriggerDelete((VortexElement) object) :
                   addIndexAndTriggerDelete((VortexIndex) object);
        }

        /**
         * Try to add element in collection waiting to be deleted
         * @param element
         * @return true if should create a new delete job, false otherwise
         */
        public boolean addElementAndTriggerDelete(VortexElement element) {
            if (this.elements.size() >= this.batchSize) {
                return true;
            }
            this.elements.add(element);
            return this.elements.size() >= this.batchSize;
        }

        /**
         * Try to add edge in collection waiting to be deleted
         * @param index
         * @return true if should create a new delete job, false otherwise
         */
        public boolean addIndexAndTriggerDelete(VortexIndex index) {
            if (this.indexes.size() >= this.batchSize) {
                return true;
            }
            this.indexes.add(index);
            return this.indexes.size() >= this.batchSize;
        }
    }
}
