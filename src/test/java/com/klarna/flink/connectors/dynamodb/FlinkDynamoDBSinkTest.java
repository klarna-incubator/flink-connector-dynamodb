/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.klarna.flink.connectors.dynamodb;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class FlinkDynamoDBSinkTest {

    @Test
    public void testSinkIsSerializable() {
        final FlinkDynamoDBSink<String> sink =
                new FlinkDynamoDBSink<>(
                        new DummyFlinkDynamoDBClientBuilder(),
                        "table",
                        DynamoDBSinkConfig.builder().build(),
                        new DummyDynamoDBWriteRequestMapper(),
                        null
                );
        assertTrue(InstantiationUtil.isSerializable(sink));
    }

    /** Test ensuring that if an invoke call happens right after an async exception is caught, it
     * should be rethrown.
     */
    @Test
    public void testAsyncErrorRethrownOnInvoke() throws Throwable {
        final DummyFlinkDynamoDBSink sink =
                new DummyFlinkDynamoDBSink(
                        new DummyFlinkDynamoDBClientBuilder(),
                        DynamoDBSinkConfig.builder().build(),
                        null
                );

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        testHarness.processElement(new StreamRecord<>("1"));

        sink.getPendingRecordFutures()
                .get(0)
                .setException(new Exception("artificial async exception"));

        try {
            testHarness.processElement(new StreamRecord<>("2"));
        } catch (Exception e) {
            // the next invoke should rethrow the async exception
            Assert.assertTrue(
                    ExceptionUtils.findThrowableWithMessage(e, "artificial async exception")
                            .isPresent());

            // test succeeded
            return;
        }

        Assert.fail();
    }

    /**
     * Test ensuring that if a snapshot call happens right after an async exception is caught, it
     * should be rethrown.
     */
    @Test
    public void testAsyncErrorRethrownOnCheckpoint() throws Throwable {
        final DummyFlinkDynamoDBSink sink =
                new DummyFlinkDynamoDBSink(
                        new DummyFlinkDynamoDBClientBuilder(),
                        DynamoDBSinkConfig.builder().build(),
                        null
                );

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));


        testHarness.open();

        testHarness.processElement(new StreamRecord<>("1"));

        sink.getPendingRecordFutures()
                .get(0)
                .setException(new Exception("artificial async exception"));

        try {
            testHarness.snapshot(123L, 123L);
        } catch (Exception e) {
            // the next checkpoint should rethrow the async exception
            Assert.assertTrue(
                    ExceptionUtils.findThrowableWithMessage(e, "artificial async exception")
                            .isPresent());

            // test succeeded
            return;
        }

        Assert.fail();
    }


    /**
     * Test ensuring that if an async exception is caught for one of the flushed requests on
     * checkpoint, it should be rethrown; we set a timeout because the test will not finish if the
     * logic is broken.
     *
     * <p>Note that this test does not test the snapshot method is blocked correctly when there are
     * pending records. The test for that is covered in testAtLeastOnceProducer.
     */
    @Test(timeout = 10000)
    public void testAsyncErrorRethrownAfterFlush() throws Throwable {
        final DummyFlinkDynamoDBSink sink =
                new DummyFlinkDynamoDBSink(
                        new DummyFlinkDynamoDBClientBuilder(),
                        DynamoDBSinkConfig.builder().build(),
                        null
                );

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));
        testHarness.open();

        testHarness.processElement(new StreamRecord<>("1"));
        testHarness.processElement(new StreamRecord<>("2"));
        testHarness.processElement(new StreamRecord<>("3"));

        // only let the first record succeed for now
        BatchResponse batchResponse = new BatchResponse(1, 1, true, null);
        sink.getPendingRecordFutures().get(0).set(batchResponse);

        CheckedThread snapshotThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        // this should block at first, since there are still two pending records
                        // that needs to be flushed
                        testHarness.snapshot(123L, 123L);
                    }
                };
        snapshotThread.start();

        // let the 2nd message fail with an async exception
        sink.getPendingRecordFutures()
                .get(1)
                .setException(new Exception("artificial async failure for 2nd message"));
        sink.getPendingRecordFutures().get(2).set(new BatchResponse(2, 1, true, null));

        try {
            snapshotThread.sync();
        } catch (Exception e) {
            // after the flush, the async exception should have been rethrown
            Assert.assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                            e, "artificial async failure for 2nd message")
                            .isPresent());

            // test succeeded
            return;
        }

        Assert.fail();
    }

    /**
     * Test ensuring that the sink is not dropping buffered records; we set a timeout because
     * the test will not finish if the logic is broken.
     */
    @Test(timeout = 10000)
    public void testAtLeastOnceProducer() throws Throwable {
        final DummyFlinkDynamoDBSink sink =
                new DummyFlinkDynamoDBSink(
                        new DummyFlinkDynamoDBClientBuilder(),
                        DynamoDBSinkConfig.builder().build(),
                        null
                );

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));
        testHarness.open();

        testHarness.processElement(new StreamRecord<>("1"));
        testHarness.processElement(new StreamRecord<>("2"));
        testHarness.processElement(new StreamRecord<>("3"));

        // start a thread to perform checkpointing
        CheckedThread snapshotThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        // this should block until all records are flushed;
                        // if the snapshot implementation returns before pending records are
                        // flushed,
                        testHarness.snapshot(123L, 123L);
                    }
                };
        snapshotThread.start();

        // before proceeding, make sure that flushing has started and that the snapshot is still
        // blocked;
        // this would block forever if the snapshot didn't perform a flush
        sink.waitUntilFlushStarted();
        Assert.assertTrue(
                "Snapshot returned before all records were flushed", snapshotThread.isAlive());

        // now, complete the callbacks
        BatchResponse batchResponse = new BatchResponse(1, 1, true, null);

        sink.getPendingRecordFutures().get(0).set(batchResponse);
        Assert.assertTrue(
                "Snapshot returned before all records were flushed", snapshotThread.isAlive());

        sink.getPendingRecordFutures().get(1).set(batchResponse);
        Assert.assertTrue(
                "Snapshot returned before all records were flushed", snapshotThread.isAlive());

        sink.getPendingRecordFutures().get(2).set(batchResponse);

        // this would fail with an exception if flushing wasn't completed before the snapshot method
        // returned
        snapshotThread.sync();

        testHarness.close();
    }

    /**
     * Test ensuring that the sink blocks if the queue limit is exceeded, until the queue length
     * drops below the limit; we set a timeout because the test will not finish if the logic is
     * broken.
     */
    @Test(timeout = 10000)
    public void testBackpressure() throws Throwable {
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10));

        final DummyFlinkDynamoDBSink sink =
                new DummyFlinkDynamoDBSink(
                        new DummyFlinkDynamoDBClientBuilder(),
                        DynamoDBSinkConfig.builder()
                                .queueLimit(1)
                                .build(),
                        null
                );

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));
        testHarness.open();

        BatchResponse batchResponse = new BatchResponse(1, 1, true, null);

        CheckedThread msg1 =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.processElement(new StreamRecord<>("1"));
                    }
                };
        msg1.start();
        msg1.trySync(deadline.timeLeftIfAny().toMillis());
        assertFalse("Flush triggered before reaching queue limit", msg1.isAlive());

        // consume msg-1 so that queue is empty again
        sink.getPendingRecordFutures().get(0).set(batchResponse);

        CheckedThread msg2 =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.processElement(new StreamRecord<>("2"));
                    }
                };
        msg2.start();
        msg2.trySync(deadline.timeLeftIfAny().toMillis());
        assertFalse("Flush triggered before reaching queue limit", msg2.isAlive());

        CheckedThread moreElementsThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.processElement(new StreamRecord<>("3"));
                        testHarness.processElement(new StreamRecord<>("4"));
                    }
                };
        moreElementsThread.start();

        assertTrue("Producer should still block, but doesn't", moreElementsThread.isAlive());

        // consume msg-2 from the queue, leaving msg-3 in the queue and msg-4 blocked
        while (sink.getPendingRecordFutures().size() < 2) {
            Thread.sleep(50);
        }
        sink.getPendingRecordFutures().get(1).set(batchResponse);

        assertTrue("Producer should still block, but doesn't", moreElementsThread.isAlive());

        // consume msg-3, blocked msg-4 can be inserted into the queue and block is released
        while (sink.getPendingRecordFutures().size() < 3) {
            Thread.sleep(50);
        }
        sink.getPendingRecordFutures().get(2).set(batchResponse);

        moreElementsThread.trySync(deadline.timeLeftIfAny().toMillis());

        assertFalse(
                "Prodcuer still blocks although the queue is flushed",
                moreElementsThread.isAlive());

        sink.getPendingRecordFutures().get(3).set(batchResponse);

        testHarness.close();
    }

    private static class DummyFlinkDynamoDBSink extends FlinkDynamoDBSink<String> {

        private static final long serialVersionUID = -1212425318784651817L;

        private transient DynamoDBProducer mockProducer;
        private List<SettableFuture<BatchResponse>> pendingRecordFutures = new LinkedList<>();

        private transient MultiShotLatch flushLatch;

        DummyFlinkDynamoDBSink(FlinkDynamoDBClientBuilder flinkDynamoDBClientBuilder,
                               DynamoDBSinkConfig dynamoDBSinkConfig,
                               KeySelector<WriteRequest, String> keySelector) {
            super(flinkDynamoDBClientBuilder,
                    "tableName",
                    dynamoDBSinkConfig,
                    new DummyDynamoDBWriteRequestMapper(),
                    keySelector);

            // set up mock producer
            this.mockProducer = new DummyDynamoDBProducer(flinkDynamoDBClientBuilder,
                    keySelector,
                    dynamoDBSinkConfig.getBatchSize());

            this.flushLatch = new MultiShotLatch();
        }

        private class DummyDynamoDBProducer extends DynamoDBProducer {

            public DummyDynamoDBProducer(FlinkDynamoDBClientBuilder flinkDynamoDBClientBuilder,
                                         KeySelector<WriteRequest, String> keySelector,
                                         int batchSize) {
                super(flinkDynamoDBClientBuilder, keySelector, batchSize);
            }

            @Override
            public int getOutstandingRecordsCount() {
                return getNumPendingRecordFutures();
            }

            @Override
            public ListenableFuture<BatchResponse> add(DynamoDBWriteRequest dynamoDBWriteRequest) {
                SettableFuture<BatchResponse> future =
                        SettableFuture.create();
                pendingRecordFutures.add(future);
                return future;
            }

            public void flush() {
                flushLatch.trigger();
            }
        }


        @Override
        protected DynamoDBProducer getDynamoDBProducer() {
            return mockProducer;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            super.snapshotState(context);

            // if the snapshot implementation doesn't wait until all pending records are flushed, we
            // should fail the test
            if (mockProducer.getOutstandingRecordsCount() > 0) {
                throw new RuntimeException(
                        "Flushing is enabled; snapshots should be blocked until all pending records are flushed");
            }
        }

        List<SettableFuture<BatchResponse>> getPendingRecordFutures() {
            return pendingRecordFutures;
        }

        void waitUntilFlushStarted() throws Exception {
            flushLatch.await();
        }

        private int getNumPendingRecordFutures() {
            int numPending = 0;

            for (SettableFuture<BatchResponse> future : pendingRecordFutures) {
                if (!future.isDone()) {
                    numPending++;
                }
            }

            return numPending;
        }
    }

    private static class DummyDynamoDBWriteRequestMapper implements DynamoDBSinkWriteRequestMapper<String> {

        @Override
        public WriteRequest map(String s) {
            return WriteRequest.builder().build();
        }
    }

    private static class DummyFlinkDynamoDBClientBuilder implements FlinkDynamoDBClientBuilder {

        @Override
        public DynamoDbClient build() {
            return null;
        }
    }
}
