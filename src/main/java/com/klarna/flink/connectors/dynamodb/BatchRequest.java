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

import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.List;
import java.util.Map;

public class BatchRequest {

    private final Map<String, List<WriteRequest>> batch;
    private final int batchSize;
    private final long batchId;

    public BatchRequest(final long batchId, final Map<String, List<WriteRequest>> batch, int batchSize) {
        this.batchId = batchId;
        this.batch = batch;
        this.batchSize = batchSize;
    }

    public Map<String, List<WriteRequest>> getBatch() {
        return batch;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getbatchId() {
        return batchId;
    }

}
