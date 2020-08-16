package com.klarna.flink.connectors.dynamodb;

public class BatchResponse {

    private boolean success;

    private Throwable t;

    public BatchResponse(boolean success, Throwable t) {
        this.success = success;
        this.t = t;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Throwable getT() {
        return t;
    }

    public void setT(Throwable t) {
        this.t = t;
    }
}
