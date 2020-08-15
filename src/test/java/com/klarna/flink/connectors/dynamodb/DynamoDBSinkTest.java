package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(5)
public class DynamoDBSinkTest {

    @Test
    void testSuccessfulPath() throws Exception {
        try (TestDynamoDBSink testDynamoDBSink = createOpenedSink()) {
            testDynamoDBSink.enqueueListenableFuture(Futures.immediateFuture(null));
            final int originalPermits = testDynamoDBSink.getAvailablePermits();
            assertTrue(originalPermits > 0);
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
            testDynamoDBSink.invoke(new DynamoDBWriteRequest("table",
                    new WriteRequest()), null);
            assertEquals(originalPermits, testDynamoDBSink.getAvailablePermits());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
        }
    }

    @Test
    public void testThrowErrorOnInvoke() throws Exception {
        try (TestDynamoDBSink testDynamoDBSink = createOpenedSink()) {
            Exception cause = new RuntimeException();
            testDynamoDBSink.enqueueListenableFuture(Futures.immediateFailedFuture(cause));

            testDynamoDBSink.invoke(new DynamoDBWriteRequest("table",
                    new WriteRequest()), null);

            Exception e = assertThrows(IOException.class, () -> {
                testDynamoDBSink.invoke(new DynamoDBWriteRequest("table",
                        new WriteRequest()), null);
            });
            assertEquals(cause, e.getCause());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
        }
    }

    @Test
    void testThrowErrorOnClose() throws Exception {
        try(TestDynamoDBSink testDynamoDBSink = createOpenedSink()) {
            Exception cause = new RuntimeException();
            testDynamoDBSink.enqueueListenableFuture(Futures.immediateFailedFuture(cause));
            testDynamoDBSink.invoke(new DynamoDBWriteRequest("table",
                    new WriteRequest()), null);
            IOException e = assertThrows(IOException.class, () -> {
                testDynamoDBSink.close();
            });
            assertEquals(cause, e.getCause());
        }
    }

    @Test
    void testReleaseOnSuccess() throws Exception {
        final DynamoDBSinkConfig config = DynamoDBSinkConfig.builder()
                .maxConcurrentRequests(1)
                .batchSize(1)
                .build();

        try (TestDynamoDBSink testDynamoDBSink = createOpenedSink(config)) {
            assertEquals(1, testDynamoDBSink.getAvailablePermits());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());

            SettableFuture<BatchResponse> settableFuture = SettableFuture.create();
            testDynamoDBSink.enqueueListenableFuture(settableFuture);
            testDynamoDBSink.invoke(new DynamoDBWriteRequest("table",
                    new WriteRequest()), null);

            assertEquals(0, testDynamoDBSink.getAvailablePermits());
            assertEquals(1, testDynamoDBSink.getAcquiredPermits());

            settableFuture.set(null);

            assertEquals(1, testDynamoDBSink.getAvailablePermits());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
        }
    }

    @Test
    void testReleaseOnFailure() throws Exception {
        final DynamoDBSinkConfig config = DynamoDBSinkConfig.builder()
                .maxConcurrentRequests(1)
                .batchSize(1)
                .build();
        final DynamoDBFailureHandler failureHandler = ignored -> {};

        try (TestDynamoDBSink testDynamoDBSink = createOpenedSink(config, failureHandler)) {
            assertEquals(1, testDynamoDBSink.getAvailablePermits());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());

            SettableFuture<BatchResponse> settableFuture = SettableFuture.create();
            testDynamoDBSink.enqueueListenableFuture(settableFuture);
            testDynamoDBSink.invoke(new DynamoDBWriteRequest("table",
                    new WriteRequest()), null);

            assertEquals(0, testDynamoDBSink.getAvailablePermits());
            assertEquals(1, testDynamoDBSink.getAcquiredPermits());

            settableFuture.setException(new RuntimeException("Exception"));

            assertEquals(1, testDynamoDBSink.getAvailablePermits());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
        }
    }

    private TestDynamoDBSink createOpenedSink() throws Exception {
        TestDynamoDBSink testDynamoDBSink = new TestDynamoDBSink();
        testDynamoDBSink.open(new Configuration());
        return testDynamoDBSink;
    }

    private TestDynamoDBSink createOpenedSink(DynamoDBSinkConfig dynamoDBSinkConfig) throws Exception {
        TestDynamoDBSink testDynamoDBSink = new TestDynamoDBSink(dynamoDBSinkConfig);
        testDynamoDBSink.open(new Configuration());
        return testDynamoDBSink;
    }

    private TestDynamoDBSink createOpenedSink(DynamoDBSinkConfig dynamoDBSinkConfig,
                                              DynamoDBFailureHandler dynamoDBFailureHandler) throws Exception {
        TestDynamoDBSink testDynamoDBSink = new TestDynamoDBSink(dynamoDBSinkConfig, dynamoDBFailureHandler);
        testDynamoDBSink.open(new Configuration());
        return testDynamoDBSink;
    }

    private OneInputStreamOperatorTestHarness<String, Object> createOpenedTestHarness(
            TestDynamoDBSink testDynamoDBSink) throws Exception {
        final StreamSink testStreamSink = new StreamSink(testDynamoDBSink);
        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(testStreamSink);
        testHarness.open();
        return testHarness;
    }

    private static class TestDynamoDBSink extends DynamoDBSink implements AutoCloseable {

        private final static AmazonDynamoDB amazonDynamoDB = Mockito.mock(AmazonDynamoDB.class);
        private final static DynamoDBWriterBuilder DYNAMO_DB_WRITER_BUILDER;

        static {
            DYNAMO_DB_WRITER_BUILDER = new DynamoDBWriterBuilder() {

                @Override
                public DynamoDBWriter getAmazonDynamoDB() {
                    return new TestDynamoDBWriter(amazonDynamoDB);
                }

                @Override
                protected AmazonDynamoDB build(AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder) {
                    return amazonDynamoDB;
                }
            };
        }

        public TestDynamoDBSink() {
            this(DynamoDBSinkConfig.builder().batchSize(1).build());
        }

        public TestDynamoDBSink(DynamoDBSinkConfig dynamoDBSinkConfig) {
            this(DYNAMO_DB_WRITER_BUILDER, dynamoDBSinkConfig, new NoOpDynamoDBFailureHandler());
        }

        public TestDynamoDBSink(DynamoDBSinkConfig dynamoDBSinkConfig, DynamoDBFailureHandler dynamoDBFailureHandler) {
            this(DYNAMO_DB_WRITER_BUILDER, dynamoDBSinkConfig, dynamoDBFailureHandler);
        }

        public TestDynamoDBSink(DynamoDBWriterBuilder dynamoDBWriterBuilder,
                                DynamoDBSinkConfig dynamoDBSinkConfig,
                                DynamoDBFailureHandler dynamoDBFailureHandler) {
            super(dynamoDBWriterBuilder, dynamoDBSinkConfig, dynamoDBFailureHandler);
        }

        void enqueueListenableFuture(ListenableFuture<BatchResponse> listenableFuture) {
            ((TestDynamoDBWriter) dynamoDBWriter).enqueueListenableFuture(listenableFuture);
        }

    }

    private static class TestDynamoDBWriter extends DynamoDBWriter {

        private final Queue<ListenableFuture<BatchResponse>> resultSetFutures = new LinkedList<>();

        public TestDynamoDBWriter(AmazonDynamoDB amazonDynamoDB) {
            super(amazonDynamoDB);
        }

        public ListenableFuture<BatchResponse> batchWrite(final BatchRequest batchRequest) {
            return resultSetFutures.poll();
        }

        void enqueueListenableFuture(ListenableFuture<BatchResponse> listenableFuture) {
            resultSetFutures.offer(listenableFuture);
        }

    }



}
