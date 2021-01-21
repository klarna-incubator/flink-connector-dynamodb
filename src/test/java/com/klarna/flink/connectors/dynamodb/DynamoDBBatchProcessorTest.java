package com.klarna.flink.connectors.dynamodb;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.LinkedList;
import java.util.Queue;

public class DynamoDBBatchProcessorTest {

    private MultiShotLatch latch = new MultiShotLatch();

    @Test(timeout = 5000)
    public void testFlushPromotesIfThereAreRecords() throws Exception {
        DummyDynamoDBBatchProcessor processor = new DummyDynamoDBBatchProcessor(1, 10, new DummyListener());
        processor.open();
        processor.enqueueBatchResponseFuture(Futures.immediateFuture(BatchResponse.success(1)));
        processor.add(new DynamoDBWriteRequest("test_table", WriteRequest.builder().build()));
        // add should not promote since number of records added is less than batchSize
        Assert.assertEquals(0, processor.getQueue().size());
        processor.flush();
        // flush should promote when number of records added is greater than 0
        Assert.assertEquals(1, processor.getQueue().size());
        latch.await();
        Assert.assertEquals(0, processor.getQueue().size());
        processor.close();
    }

    @Test(timeout = 5000)
    public void testBatchIsPromotedWhenRecordsAddedEqualToBatchSize() throws Exception {
        final SettableFuture<BatchResponse> settableFuture = SettableFuture.create();
        DummyDynamoDBBatchProcessor processor = new DummyDynamoDBBatchProcessor(1, 1, new DummyListener());
        processor.open();
        processor.enqueueBatchResponseFuture(settableFuture);
        processor.add(new DynamoDBWriteRequest("test_table", WriteRequest.builder().build()));
        // wait for the batch to be polled from the queue
        while (processor.getAvailablePermits() == 1) {
            Thread.sleep(5);
        }
        Assert.assertEquals(0, processor.getAvailablePermits());
        settableFuture.set(BatchResponse.success(1));
        latch.await();
        Assert.assertEquals(0, processor.getQueue().size());
        processor.close();
    }

    @Test(timeout = 5000)
    public void testBatchIsNotPromotedWhenRecordsAddedLessThanBatchSize() {
        DummyDynamoDBBatchProcessor processor = new DummyDynamoDBBatchProcessor(1, 10, new DummyListener());
        processor.open();
        processor.add(new DynamoDBWriteRequest("test_table", WriteRequest.builder().build()));
        Assert.assertEquals(0, processor.getQueue().size());
        Assert.assertEquals(1, processor.getAvailablePermits());
        processor.close();
    }

    @Test(timeout = 5000)
    public void testReleaseOnFailure() throws InterruptedException {
        final SettableFuture<BatchResponse> settableFuture = SettableFuture.create();
        DummyDynamoDBBatchProcessor processor = new DummyDynamoDBBatchProcessor(1, 1, new DummyListener());
        processor.open();
        processor.enqueueBatchResponseFuture(settableFuture);
        processor.add(new DynamoDBWriteRequest("test_table", WriteRequest.builder().build()));
        // wait for the batch to be polled from the queue
        while (processor.getAvailablePermits() == 1) {
            Thread.sleep(5);
        }
        Assert.assertEquals(0, processor.getAvailablePermits());
        settableFuture.setException(new Exception("Test exception"));
        latch.await();
        Assert.assertEquals(1, processor.getAvailablePermits());
        processor.close();
    }

    @Test(timeout = 5000)
    public void testReleaseOnSuccess() throws InterruptedException {
        final SettableFuture<BatchResponse> settableFuture = SettableFuture.create();
        DummyDynamoDBBatchProcessor processor = new DummyDynamoDBBatchProcessor(1, 1, new DummyListener());
        processor.open();
        processor.enqueueBatchResponseFuture(settableFuture);
        processor.add(new DynamoDBWriteRequest("test_table", WriteRequest.builder().build()));
        // wait for the batch to be polled from the queue
        while (processor.getAvailablePermits() == 1) {
            Thread.sleep(5);
        }
        Assert.assertEquals(0, processor.getAvailablePermits());
        settableFuture.set(BatchResponse.success(1));
        latch.await();
        Assert.assertEquals(1, processor.getAvailablePermits());
        processor.close();
    }

    private static  class DummyDynamoDBBatchProcessor extends DynamoDBBatchProcessor {

        private final Queue<ListenableFuture<BatchResponse>> batchResponseFutures = new LinkedList<>();

        public DummyDynamoDBBatchProcessor(int maxConcurrentRequests,
                                           int batchSize,
                                           Listener listener) {
            super(new DummyFlinkDynamoDBClientBuilder(), maxConcurrentRequests, batchSize, listener);
        }

        @Override
        protected ListenableFuture<BatchResponse> batchWrite(final BatchRequest batchRequest) {
            return batchResponseFutures.poll();
        }

        public void enqueueBatchResponseFuture(final ListenableFuture<BatchResponse> responseFuture) {
            batchResponseFutures.offer(responseFuture);
        }

    }

    private static class DummyFlinkDynamoDBClientBuilder implements FlinkDynamoDBClientBuilder {
        @Override
        public DynamoDbClient build() {
            return null;
        }
    }

    private class DummyListener implements DynamoDBBatchProcessor.Listener {

        @Override
        public void onSuccess(BatchResponse batchResponse) {
            latch.trigger();
        }

        @Override
        public void onFailure(Throwable t) {
            latch.trigger();
        }
    }

}
