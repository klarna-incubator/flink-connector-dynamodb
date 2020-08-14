package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

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
            testDynamoDBSink.invoke("N/A", null);
            assertEquals(originalPermits, testDynamoDBSink.getAvailablePermits());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
        }
    }

    @Test
    public void testThrowErrorOnInvoke() throws Exception {
        try (TestDynamoDBSink testDynamoDBSink = createOpenedSink()) {
            Exception cause = new RuntimeException();
            testDynamoDBSink.enqueueListenableFuture(Futures.immediateFailedFuture(cause));

            testDynamoDBSink.invoke("Invoke1", null);

            Exception e = assertThrows(IOException.class, () -> {
                testDynamoDBSink.invoke("Invoke2", null);
            });
            assertEquals(cause, e.getCause());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
        }
    }

//    @Test
//    void testThrowErrorOnSnapshot() throws Exception {
//        final TestDynamoDBSink testDynamoDBSink = new TestDynamoDBSink();
//
//        try (OneInputStreamOperatorTestHarness<String, Object> testHarness = createOpenedTestHarness(testDynamoDBSink)) {
//            Exception cause = new RuntimeException();
//            testDynamoDBSink.enqueueListenableFuture(Futures.immediateFailedFuture(cause));
//
//            testDynamoDBSink.invoke("N/A", null);
//
//            CheckpointException e = assertThrows(CheckpointException.class, () -> {
//                testHarness.snapshot(123L, 123L);
//            });
//
//            Optional<IOException> serializedThrowable = findSerializedThrowable(e, IOException.class, ClassLoader.getSystemClassLoader());
//
//            assertTrue(serializedThrowable.isPresent());
//
//        }
//    }

    @Test
    void testThrowErrorOnClose() throws Exception {
        try(TestDynamoDBSink testDynamoDBSink = createOpenedSink()) {
            Exception cause = new RuntimeException();
            testDynamoDBSink.enqueueListenableFuture(Futures.immediateFailedFuture(cause));
            testDynamoDBSink.invoke("N/A", null);
            IOException e = assertThrows(IOException.class, () -> {
                testDynamoDBSink.close();
            });
            assertEquals(cause, e.getCause());
        }
    }

    @Test
    void testWaitForPendingUpdatesOnSnapshot() throws Exception {
        final TestDynamoDBSink testDynamoDBSink = new TestDynamoDBSink();

        try (OneInputStreamOperatorTestHarness<String, Object> testHarness = createOpenedTestHarness(testDynamoDBSink)) {
            SettableFuture<String> settableFuture = SettableFuture.create();
            testDynamoDBSink.enqueueListenableFuture(settableFuture);

            testDynamoDBSink.invoke("N/A", null);
            assertEquals(1, testDynamoDBSink.getAcquiredPermits());

            final CountDownLatch latch = new CountDownLatch(1);
            Thread t = new CheckedThread("Flink-DynamoDbSinkBaseTest") {
                @Override
                public void go() throws Exception {
                    testHarness.snapshot(123L, 123L);
                    latch.countDown();
                }
            };
            t.start();
            while (t.getState() != Thread.State.TIMED_WAITING) {
                Thread.sleep(5);
            }

            assertEquals(1, testDynamoDBSink.getAcquiredPermits());
            settableFuture.set(null);
            latch.await();
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
        }
    }

    @Test
    void testWaitForPendingUpdatesOnClose() throws Exception {
        final TestDynamoDBSink testDynamoDBSink = new TestDynamoDBSink();

        try (OneInputStreamOperatorTestHarness<String, Object> testHarness = createOpenedTestHarness(testDynamoDBSink)) {

            SettableFuture<String> settableFuture = SettableFuture.create();
            testDynamoDBSink.enqueueListenableFuture(settableFuture);

            testDynamoDBSink.invoke("N/A", null);
            assertEquals(1, testDynamoDBSink.getAcquiredPermits());

            final CountDownLatch latch = new CountDownLatch(1);
            Thread t = new CheckedThread("Flink-DynamoDBSinkBaseTest") {
                @Override
                public void go() throws Exception {
                    testHarness.close();
                    latch.countDown();
                }
            };
            t.start();
            while (t.getState() != Thread.State.TIMED_WAITING) {
                Thread.sleep(5);
            }

            assertEquals(1, testDynamoDBSink.getAcquiredPermits());
            settableFuture.set(null);
            latch.await();
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
        }
    }

    @Test
    void testReleaseOnSuccess() throws Exception {
        final DynamoDBSinkBaseConfig config = DynamoDBSinkBaseConfig.builder()
                .maxConcurrentRequests(1)
                .build();

        try (TestDynamoDBSink testDynamoDBSink = createOpenedSink(config)) {
            assertEquals(1, testDynamoDBSink.getAvailablePermits());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());

            SettableFuture<String> settableFuture = SettableFuture.create();
            testDynamoDBSink.enqueueListenableFuture(settableFuture);
            testDynamoDBSink.invoke("N/A", null);

            assertEquals(0, testDynamoDBSink.getAvailablePermits());
            assertEquals(1, testDynamoDBSink.getAcquiredPermits());

            settableFuture.set(null);

            assertEquals(1, testDynamoDBSink.getAvailablePermits());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
        }
    }

    @Test
    void testReleaseOnFailure() throws Exception {
        final DynamoDBSinkBaseConfig config = DynamoDBSinkBaseConfig.builder()
                .maxConcurrentRequests(1)
                .build();
        final DynamoDBFailureHandler failureHandler = ignored -> {};

        try (TestDynamoDBSink testDynamoDBSink = createOpenedSink(config, failureHandler)) {
            assertEquals(1, testDynamoDBSink.getAvailablePermits());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());

            SettableFuture<String> settableFuture = SettableFuture.create();
            testDynamoDBSink.enqueueListenableFuture(settableFuture);
            testDynamoDBSink.invoke("N/A", null);

            assertEquals(0, testDynamoDBSink.getAvailablePermits());
            assertEquals(1, testDynamoDBSink.getAcquiredPermits());

            settableFuture.setException(new RuntimeException("Exception"));

            assertEquals(1, testDynamoDBSink.getAvailablePermits());
            assertEquals(0, testDynamoDBSink.getAcquiredPermits());
        }
    }

    @Test
    void testReleaseOnThrowingSend() throws Exception {
        final DynamoDBSinkBaseConfig config = DynamoDBSinkBaseConfig.builder()
                .maxConcurrentRequests(1)
                .build();

        try (TestDynamoDBSink testDynamoDBSink = new TestDynamoDBSink(config) {
            protected ListenableFuture<String> send(String value) {
                throw new RuntimeException("Error in send");
            }
        }) {
            testDynamoDBSink.open(new Configuration());
            assertEquals(testDynamoDBSink.getAvailablePermits(), 1);
            assertEquals(testDynamoDBSink.getAcquiredPermits(), 0);

            assertThrows(RuntimeException.class, () -> {
                testDynamoDBSink.invoke("N/A", null);
            });
            assertEquals(testDynamoDBSink.getAvailablePermits(), 1);
            assertEquals(testDynamoDBSink.getAcquiredPermits(), 0);
        }
    }

    @Test
    void testTimeoutExceptionOnInvoke() throws Exception {
        final DynamoDBSinkBaseConfig config = DynamoDBSinkBaseConfig.builder()
                .maxConcurrentRequests(1)
                .maxConcurrentRequestsTimeout(Duration.ofMillis(1))
                .build();
        try (TestDynamoDBSink testDynamoDBSink = createOpenedSink(config)) {
            SettableFuture<String> settableFuture = SettableFuture.create();
            testDynamoDBSink.enqueueListenableFuture(settableFuture);
            testDynamoDBSink.enqueueListenableFuture(settableFuture);
            testDynamoDBSink.invoke("Invoke1", null);

            assertThrows(TimeoutException.class, () -> {
                testDynamoDBSink.invoke("Invoke2", null);
            });
            settableFuture.set(null);
        }
    }

    private TestDynamoDBSink createOpenedSink() throws Exception {
        TestDynamoDBSink testDynamoDBSink = new TestDynamoDBSink();
        testDynamoDBSink.open(new Configuration());
        return testDynamoDBSink;
    }

    private TestDynamoDBSink createOpenedSink(DynamoDBSinkBaseConfig dynamoDBSinkBaseConfig) throws Exception {
        TestDynamoDBSink testDynamoDBSink = new TestDynamoDBSink(dynamoDBSinkBaseConfig);
        testDynamoDBSink.open(new Configuration());
        return testDynamoDBSink;
    }

    private TestDynamoDBSink createOpenedSink(DynamoDBSinkBaseConfig dynamoDBSinkBaseConfig,
                                              DynamoDBFailureHandler dynamoDBFailureHandler) throws Exception {
        TestDynamoDBSink testDynamoDBSink = new TestDynamoDBSink(dynamoDBSinkBaseConfig, dynamoDBFailureHandler);
        testDynamoDBSink.open(new Configuration());
        return testDynamoDBSink;
    }

    private OneInputStreamOperatorTestHarness<String, Object> createOpenedTestHarness(
            TestDynamoDBSink testDynamoDBSink) throws Exception {
        final StreamSink<String> testStreamSink = new StreamSink<>(testDynamoDBSink);
        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(testStreamSink);
        testHarness.open();
        return testHarness;
    }

    private static class TestDynamoDBSink extends DynamoDBSink<String, String> implements AutoCloseable {

        private final static AmazonDynamoDB amazonDynamoDB;
        private final static DynamoDBBuilder dynamoDBBuilder;

        static {
            amazonDynamoDB = Mockito.mock(AmazonDynamoDB.class);
            dynamoDBBuilder = new DynamoDBBuilder() {
                @Override
                protected AmazonDynamoDB build(AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder) {
                    return amazonDynamoDB;
                }
            };
        }

        private final Queue<ListenableFuture<String>> resultSetFutures = new LinkedList<>();

        public TestDynamoDBSink() {
            this(DynamoDBSinkBaseConfig.builder().build());
        }

        public TestDynamoDBSink(DynamoDBSinkBaseConfig dynamoDBSinkBaseConfig) {
            this(dynamoDBBuilder, dynamoDBSinkBaseConfig, new NoOpDynamoDBFailureHandler());
        }

        public TestDynamoDBSink(DynamoDBSinkBaseConfig dynamoDBSinkBaseConfig, DynamoDBFailureHandler dynamoDBFailureHandler) {
            this(dynamoDBBuilder, dynamoDBSinkBaseConfig, dynamoDBFailureHandler);
        }

        public TestDynamoDBSink(DynamoDBBuilder dynamoDBBuilder,
                                DynamoDBSinkBaseConfig dynamoDBSinkBaseConfig,
                                DynamoDBFailureHandler dynamoDBFailureHandler) {
            super(dynamoDBBuilder, dynamoDBSinkBaseConfig, dynamoDBFailureHandler);
        }

        @Override
        protected ListenableFuture<String> send(String value) {
            return resultSetFutures.poll();
        }

        void enqueueListenableFuture(ListenableFuture<String> listenableFuture) {
            resultSetFutures.offer(listenableFuture);
        }
    }

}
