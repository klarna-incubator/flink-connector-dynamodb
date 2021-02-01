package com.klarna.flink.connectors.dynamodb.utils;

public class TimeoutLatch {
    private final Object lock = new Object();
    private volatile boolean waiting;

    public void await(long timeout) throws InterruptedException {
        synchronized (lock) {
            waiting = true;
            lock.wait(timeout);
        }
    }

    public void trigger() {
        if (waiting) {
            synchronized (lock) {
                waiting = false;
                lock.notifyAll();
            }
        }
    }
}