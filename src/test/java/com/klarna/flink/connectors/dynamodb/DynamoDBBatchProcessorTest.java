package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;


public class DynamoDBBatchProcessorTest {

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Test
    public void whenAddWithBatchSize1OutBatchesShouldBeEqualTo1() throws InterruptedException {
        DummyDynamoDBBatchProcessor dummyDynamoDBBatchProcessor = createOpenedBatchProcessor(1, 1);
        dummyDynamoDBBatchProcessor.enqueueListenableFuture(Futures.immediateFuture(new BatchResponse(true, null)));
        assertEquals(0, dummyDynamoDBBatchProcessor.getOutBatches());
        assertEquals(1, dummyDynamoDBBatchProcessor.getAvailablePermits());
        dummyDynamoDBBatchProcessor.add(new DynamoDBWriteRequest("tableName", new WriteRequest()));

        assertEquals(1, dummyDynamoDBBatchProcessor.getOutBatches());
        countDownLatch.await();

        assertEquals(0, dummyDynamoDBBatchProcessor.getOutBatches());
        assertEquals(1, dummyDynamoDBBatchProcessor.getAvailablePermits());

        dummyDynamoDBBatchProcessor.close();
    }

    @Test
    public void whenAddWithBatchSize2OutBatchesShouldBeEqualTo0() throws InterruptedException {
        DummyDynamoDBBatchProcessor dummyDynamoDBBatchProcessor = createOpenedBatchProcessor(1, 2);
        dummyDynamoDBBatchProcessor.enqueueListenableFuture(Futures.immediateFuture(new BatchResponse(true, null)));
        assertEquals(0, dummyDynamoDBBatchProcessor.getOutBatches());
        dummyDynamoDBBatchProcessor.add(new DynamoDBWriteRequest("tableName", new WriteRequest()));
        assertEquals(0, dummyDynamoDBBatchProcessor.getOutBatches());
        dummyDynamoDBBatchProcessor.close();
    }

    @Test
    public void whenCurrentNumberOfRequestsLessThanBatchSizeFlushShouldPromote() throws Exception {
        DummyDynamoDBBatchProcessor dummyDynamoDBBatchProcessor = createOpenedBatchProcessor(1, 2);
        dummyDynamoDBBatchProcessor.enqueueListenableFuture(Futures.immediateFuture(new BatchResponse(true, null)));
        assertEquals(0, dummyDynamoDBBatchProcessor.getOutBatches());
        dummyDynamoDBBatchProcessor.add(new DynamoDBWriteRequest("tableName", new WriteRequest()));
        assertEquals(0, dummyDynamoDBBatchProcessor.getOutBatches());
        dummyDynamoDBBatchProcessor.flush();
        assertEquals(1, dummyDynamoDBBatchProcessor.getOutBatches());
        dummyDynamoDBBatchProcessor.close();
    }

    private DummyDynamoDBBatchProcessor createOpenedBatchProcessor(int maxConcurrentRequests, int batchSize) {
        DummyDynamoDBBatchProcessor dummyDynamoDBBatchProcessor = new DummyDynamoDBBatchProcessor(new DummyAmazonDynamoDBBuilder()
                , maxConcurrentRequests, batchSize, new DummyDynamoDBBatchProcessorListener());
        dummyDynamoDBBatchProcessor.open();
        return dummyDynamoDBBatchProcessor;
    }

    private class DummyDynamoDBBatchProcessor extends DynamoDBBatchProcessor {

        private final Queue<ListenableFuture<BatchResponse>> resultSetFutures = new LinkedList<>();

        public DummyDynamoDBBatchProcessor(AmazonDynamoDBBuilder amazonDynamoDBBuilder,
                                           int maxConcurrentRequests,
                                           int batchSize,
                                           Listener listener) {
            super(amazonDynamoDBBuilder, maxConcurrentRequests, batchSize, listener);
        }

        protected ListenableFuture<BatchResponse> batchWrite(final BatchRequest batchRequest) {
            return resultSetFutures.poll();
        }

        public void enqueueListenableFuture(ListenableFuture<BatchResponse> listenableFuture) {
            resultSetFutures.offer(listenableFuture);
        }
    }

    private class DummyDynamoDBBatchProcessorListener implements DynamoDBBatchProcessor.Listener {

        @Override
        public void onSuccess(BatchResponse batchResponse) {
            countDownLatch.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
            countDownLatch.countDown();
        }
    }

    private class DummyAmazonDynamoDBBuilder implements AmazonDynamoDBBuilder {

        @Override
        public AmazonDynamoDB build() {
            return null;
        }
    }

}
