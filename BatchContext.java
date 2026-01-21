package com.smt.memorytest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchContext {
	
	enum State { STARTED, COMPLETED, CANCELLED }

    private State state = State.STARTED;
    final Map<String, PriceRecord> records = new HashMap<>();

    synchronized void addRecords(List<PriceRecord> chunk) {
        ensureStarted();
        chunk.forEach(r ->
                records.merge(
                        r.id(),
                        r,
                        (oldVal, newVal) ->
                                newVal.asOf().isAfter(oldVal.asOf()) ? newVal : oldVal
                )
        );
    }

    synchronized void markCompleted() {
        ensureStarted();
        state = State.COMPLETED;
    }

    synchronized void markCancelled() {
        ensureStarted();
        state = State.CANCELLED;
        records.clear();
    }

    boolean isActive() {
        return state == State.STARTED;
    }

    private void ensureStarted() {
        if (state != State.STARTED) {
            throw new IllegalStateException("Batch already for state testing smt global " + state);
        }
    }

}
