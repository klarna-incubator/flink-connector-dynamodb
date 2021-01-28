package com.klarna.flink.connectors.dynamodb;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.flink.api.java.functions.KeySelector;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotSame;
import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;

public class DynamoDBProducerTest {

    @Test
    public void testExecutorPollingBatch() throws InterruptedException, ExecutionException, TimeoutException {
        DummyDynamoDBProducer producer =
                new DummyDynamoDBProducer(
                        new DummyFlinkDynamoDBClientBuilder(),
                        null,
                        25);
        List<ListenableFuture<BatchResponse>> futures = new ArrayList<>();
        ListenableFuture<BatchResponse> f;
        for (int i = 0; i < 25; i++) {
            f = producer.add(new DynamoDBWriteRequest("YY", WriteRequest.builder().build()));
            futures.add(f);
        }
        f = producer.add(new DynamoDBWriteRequest("YY", WriteRequest.builder().build()));
        futures.add(f);
        futures.get(0).get(1000, TimeUnit.MILLISECONDS);
        //consuming batches of 25
        assertEquals(1, producer.getOutstandingRecordsCount());
        //one in the currentmap
        assertEquals(1,
                producer.getUnderConstruction().values().stream()
                        .mapToInt(List::size)
                        .sum());
        //no map in the queue
        assertEquals(0, producer.getQueue().size());
        //all the futures are done but the last
        assertFalse(futures.get(futures.size() - 1).isDone());
        for (int i = 0; i < futures.size() - 1; i++) {
            assertTrue(futures.get(i).isDone());
        }
        producer.destroy();
    }

    @Test
    public void testAddReturnsSameFutureForBatch() {
        DummyDynamoDBProducer producer =
                new DummyDynamoDBProducer(
                        new DummyFlinkDynamoDBClientBuilder(),
                        null,
                        25);
        ListenableFuture<BatchResponse> first = producer.add(new DynamoDBWriteRequest("YY", WriteRequest.builder().build()));
        for (int i = 1; i < 25; i++) {
            ListenableFuture<BatchResponse> f = producer.add(new DynamoDBWriteRequest("YY", WriteRequest.builder().build()));
            assertSame(f, first);
        }
        ListenableFuture<BatchResponse> different = producer.add(new DynamoDBWriteRequest("YY", WriteRequest.builder().build()));
        assertNotSame(different, first);
    }

    @Test
    public void testFlush() throws InterruptedException, ExecutionException, TimeoutException {
        DummyDynamoDBProducer producer =
                new DummyDynamoDBProducer(new DummyFlinkDynamoDBClientBuilder(),
                        null,
                        25);
        ListenableFuture<BatchResponse> f;
        f = producer.add(new DynamoDBWriteRequest("YY", WriteRequest.builder().build()));
        //spiller consumer a map of 25
        assertEquals(1, producer.getOutstandingRecordsCount());
        //one in the currentmap
        assertEquals(1,
                producer.getUnderConstruction().values().stream()
                        .mapToInt(List::size)
                        .sum());
        //no map in the queue
        assertEquals(0, producer.getQueue().size());
        //call flush
        producer.flush();
        //no map in the queue
        assertEquals(1, producer.getQueue().size());
        f.get(1000, TimeUnit.MILLISECONDS);
        assertTrue(f.isDone());
        producer.destroy();
    }

    private static class DummyDynamoDBProducer extends DynamoDBProducer {

        public DummyDynamoDBProducer(FlinkDynamoDBClientBuilder flinkDynamoDBClientBuilder,
                                     KeySelector<DynamoDBWriteRequest, String> keySelector,
                                     int batchSize) {
            super(flinkDynamoDBClientBuilder, keySelector, batchSize);
        }

        @Override
        protected Callable<BatchResponse> batchWrite(final BatchRequest batchRequest) {
            return () -> {
              return new BatchResponse(batchRequest.getbatchId(), batchRequest.getBatchSize(), true, null);
            };
        }
    }

    private static class DummyFlinkDynamoDBClientBuilder implements FlinkDynamoDBClientBuilder {
        @Override
        public DynamoDbClient build() {
            return null;
        }
    }

}
