package com.smt.memorytest;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryPriceService implements PriceService {

	private final Map<String, PriceRecord> livePrices = new ConcurrentHashMap<>();
    private final Map<UUID, BatchContext> batches = new ConcurrentHashMap<>();

    @Override
    public UUID startBatch() {
        UUID batchId = UUID.randomUUID();
        batches.put(batchId, new BatchContext());
        return batchId;
    }

    @Override
    public void uploadChunk(UUID batchId, List<PriceRecord> records) {
        BatchContext ctx = getActiveBatch(batchId);
        ctx.addRecords(records);
    }

    @Override
    public void completeBatch(UUID batchId) {
        BatchContext ctx = getActiveBatch(batchId);

        synchronized (ctx) {
            ctx.markCompleted();

            ctx.records.forEach((id, record) ->
                    livePrices.merge(id, record,
                            (oldVal, newVal) ->
                                    newVal.asOf().isAfter(oldVal.asOf()) ? newVal : oldVal
                    )
            );
        }
        batches.remove(batchId);
    }

    @Override
    public void cancelBatch(UUID batchId) {
        BatchContext ctx = getActiveBatch(batchId);
        ctx.markCancelled();
        batches.remove(batchId);
    }

    @Override
    public Optional<PriceRecord> getLastPrice(String instrumentId) {
        return Optional.ofNullable(livePrices.get(instrumentId));
    }

    private BatchContext getActiveBatch(UUID batchId) {
        BatchContext ctx = batches.get(batchId);
        if (ctx == null || !ctx.isActive()) {
            throw new IllegalStateException("smt global test Invalid or inactive batch: " + batchId);
        }
        return ctx;
    }

}


