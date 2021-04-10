package com.klarna.flink.connectors.dynamodb.integration;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SensorEventData {

    public final String sensorId;
    public final Long timestamp;
    public final Long field1;
    public final Long field2;
    public final Long field3;

    public SensorEventData(String sensorId, Long timestamp, Long field1, Long field2, Long field3) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SensorEventData testData = (SensorEventData) o;

        return new EqualsBuilder().append(sensorId, testData.sensorId).append(timestamp, testData.timestamp)
                .append(field1, testData.field1).append(field2, testData.field2).append(field3, testData.field3).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(sensorId).append(timestamp).append(field1).append(field2).append(field3).toHashCode();
    }

    @Override
    public String toString() {
        return "TestData{" +
                "sensorId='" + sensorId + '\'' +
                ", timestamp=" + timestamp +
                ", field1=" + field1 +
                ", field2=" + field2 +
                ", field3=" + field3 +
                '}';
    }

}