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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class FlinkDynamoDBSinkTest {

    @Test
    public void testSuccessfullPath() throws Exception {
        final DummyFlinkDynamoDBSink sink = new DummyFlinkDynamoDBSink(
                new DummyAmazonDynamoDBBuilder(), DynamoDBSinkConfig.builder().build(), new NoOpDynamoDBFailureHandler());
        final OneInputStreamOperatorTestHarness<DynamoDBWriteRequest, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();
        testHarness.processElement(new StreamRecord<>(new DynamoDBWriteRequest("table", new WriteRequest())));
        Assert.assertEquals(1, sink.getNumPendingRecords());
        sink.setBatchResponse(BatchResponse.success(1));
        sink.manualBatch();
        Assert.assertEquals(0, sink.getNumPendingRecords());
    }


    @Test
    public void testFailureThrownOnInvoke() throws Exception {
        final DummyFlinkDynamoDBSink sink = new DummyFlinkDynamoDBSink(
                new DummyAmazonDynamoDBBuilder(), DynamoDBSinkConfig.builder().build(), new NoOpDynamoDBFailureHandler());
        final OneInputStreamOperatorTestHarness<DynamoDBWriteRequest, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();
        testHarness.processElement(new StreamRecord<>(new DynamoDBWriteRequest("table", new WriteRequest())));
        sink.setBatchResponse(BatchResponse.fail(new Exception("Test exception")));
        sink.manualBatch();
        try {
            testHarness.processElement(new StreamRecord<>(new DynamoDBWriteRequest("table", new WriteRequest())));
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("Test exception"));
            return;
        }
        Assert.fail();
    }

    @Test
    public void testFailureThrownOnCheckpoint() throws Exception {
        final DummyFlinkDynamoDBSink sink = new DummyFlinkDynamoDBSink(
                new DummyAmazonDynamoDBBuilder(), DynamoDBSinkConfig.builder().build(), new NoOpDynamoDBFailureHandler());
        final OneInputStreamOperatorTestHarness<DynamoDBWriteRequest, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();
        testHarness.processElement(new StreamRecord<>(new DynamoDBWriteRequest("table", new WriteRequest())));
        sink.setBatchResponse(BatchResponse.fail(new Exception("Test exception")));

        sink.manualBatch();
        try {
            testHarness.snapshot(123L, 123L);
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getCause().getMessage().contains("Test exception"));
            return;
        }
        Assert.fail();
    }

    @Test
    public void testFailureThrownOnClose() throws Exception {
        final DummyFlinkDynamoDBSink sink = new DummyFlinkDynamoDBSink(
                new DummyAmazonDynamoDBBuilder(), DynamoDBSinkConfig.builder().build(), new NoOpDynamoDBFailureHandler());
        final OneInputStreamOperatorTestHarness<DynamoDBWriteRequest, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();
        testHarness.processElement(new StreamRecord<>(new DynamoDBWriteRequest("table", new WriteRequest())));
        sink.setBatchResponse(BatchResponse.fail(new Exception("Test exception")));
        sink.manualBatch();
        try {
            testHarness.close();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("Test exception"));
            return;
        }
        Assert.fail();
    }

    @Test(timeout = 5000)
    public void testFlushOnClose() throws Exception {
        final DummyFlinkDynamoDBSink sink = new DummyFlinkDynamoDBSink(
                new DummyAmazonDynamoDBBuilder(), DynamoDBSinkConfig.builder().build(), new NoOpDynamoDBFailureHandler());
        final OneInputStreamOperatorTestHarness<DynamoDBWriteRequest, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();
        testHarness.processElement(new StreamRecord<>(new DynamoDBWriteRequest("table", new WriteRequest())));
        Assert.assertEquals(1, sink.getNumPendingRecords());
        CheckedThread snapshotThread = new CheckedThread() {
            @Override
            public void go() throws Exception {
                testHarness.close();
            }
        };
        snapshotThread.start();

        sink.setBatchResponse(BatchResponse.success(1));
        sink.manualBatch();
        Assert.assertEquals(0, sink.getNumPendingRecords());

    }

    @Test(timeout = 5000)
    public void testFlushOnSnapshot() throws Exception {
        final DummyFlinkDynamoDBSink sink = new DummyFlinkDynamoDBSink(
                new DummyAmazonDynamoDBBuilder(), DynamoDBSinkConfig.builder().build(), new NoOpDynamoDBFailureHandler());
        final OneInputStreamOperatorTestHarness<DynamoDBWriteRequest, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();
        testHarness.processElement(new StreamRecord<>(new DynamoDBWriteRequest("table", new WriteRequest())));
        Assert.assertEquals(1, sink.getNumPendingRecords());
        CheckedThread snapshotThread = new CheckedThread() {
            @Override
            public void go() throws Exception {
                testHarness.snapshot(123L, 123L);
            }
        };
        snapshotThread.start();

        sink.setBatchResponse(BatchResponse.success(1));
        sink.manualBatch();
        Assert.assertEquals(0, sink.getNumPendingRecords());

    }

    private static class DummyFlinkDynamoDBSink extends FlinkDynamoDBSink {

        private BatchResponse batchResponse;
        private DynamoDBBatchProcessor dynamoDBBatchProcessor;
        private BatchRequest batchRequest;

        private transient MultiShotLatch flushLatch = new MultiShotLatch();

        public DummyFlinkDynamoDBSink(AmazonDynamoDBBuilder amazonDynamoDBBuilder,
                                      DynamoDBSinkConfig dynamoDBSinkConfig,
                                      DynamoDBFailureHandler failureHandler) {
            super(amazonDynamoDBBuilder, dynamoDBSinkConfig, failureHandler);
        }

        public void setBatchResponse(BatchResponse batchResponse) {
            this.batchResponse = batchResponse;
        }

        public void manualBatch() {
            flushLatch.trigger();
            dynamoDBBatchProcessor.flush();
        }

        @Override
        protected DynamoDBBatchProcessor buildDynamoDBBatchProcessor(DynamoDBBatchProcessor.Listener listener) {
            this.dynamoDBBatchProcessor = new DynamoDBBatchProcessor(new DummyAmazonDynamoDBBuilder(), 0, 0, null) {

                @Override
                public void add(final DynamoDBWriteRequest dynamoDBWriteRequest) {
                    if (batchRequest == null) {
                        batchRequest = new BatchRequest(Collections.singletonMap(dynamoDBWriteRequest.getTableName(),
                                Collections.singletonList(dynamoDBWriteRequest.getWriteRequest())), 1);
                    } else {
                        batchRequest.getBatch().get(dynamoDBWriteRequest.getTableName()).add(dynamoDBWriteRequest.getWriteRequest());
                    }
                }

                @Override
                public void flush() {
                    try {
                        flushLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    if (batchResponse.getT() != null) {
                        listener.onFailure(batchResponse.getT());
                    } else {
                        listener.onSuccess(batchResponse);
                    }
                }
            };
            return this.dynamoDBBatchProcessor;
        }
    }

    private static class DummyAmazonDynamoDBBuilder implements AmazonDynamoDBBuilder {

        @Override
        public AmazonDynamoDB build() {
            return null;
        }
    }
}
