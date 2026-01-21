package com.smt.memorytest;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class InMemoryPriceServiceTest {
	
	PriceService service = new InMemoryPriceService();

    @Test
    void batchPricesBecomeVisibleOnlyOnCompletion() {
        UUID batch = service.startBatch();

        service.uploadChunk(batch, List.of(
                new PriceRecord("AAPL", Instant.parse("2024-01-01T10:00:00Z"), Map.of("price", 150))
        ));

        assertTrue(service.getLastPrice("AAPL").isEmpty());

        service.completeBatch(batch);

        assertEquals(150, service.getLastPrice("AAPL").get().payload().get("price"));
    }

    @Test
    void cancelledBatchIsDiscarded() {
        UUID batch = service.startBatch();

        service.uploadChunk(batch, List.of(
                new PriceRecord("IBM", Instant.now(), Map.of("price", 120))
        ));

        service.cancelBatch(batch);

        assertTrue(service.getLastPrice("IBM").isEmpty());
    }

    @Test
    void newerAsOfOverridesOlder() {
        UUID batch = service.startBatch();

        service.uploadChunk(batch, List.of(
                new PriceRecord("MSFT", Instant.parse("2024-01-01T10:00:00Z"), Map.of("price", 300)),
                new PriceRecord("MSFT", Instant.parse("2024-01-01T11:00:00Z"), Map.of("price", 310))
        ));

        service.completeBatch(batch);

        assertEquals(310, service.getLastPrice("MSFT").get().payload().get("price"));
    }

    @Test
    void invalidBatchUsageThrowsException() {
        UUID batch = service.startBatch();
        service.completeBatch(batch);

        assertThrows(IllegalStateException.class,
                () -> service.uploadChunk(batch, List.of()));
    }

}
