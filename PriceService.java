package com.smt.memorytest;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

interface PriceService {
	
	UUID startBatch();

    void uploadChunk(UUID batchId, List<PriceRecord> records);

    void completeBatch(UUID batchId);

    void cancelBatch(UUID batchId);

    Optional<PriceRecord> getLastPrice(String instrumentId);

}
