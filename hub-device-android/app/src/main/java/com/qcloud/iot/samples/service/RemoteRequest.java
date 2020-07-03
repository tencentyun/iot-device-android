package com.qcloud.iot.samples.service;


public class RemoteRequest {

    private int mRequestId;

    public RemoteRequest() {
    }

    public RemoteRequest(int requestId) {
        this.mRequestId = requestId;
    }

    @Override
    public String toString() {
        return "RemoteRequest{" +
                "mRequestId=" + mRequestId +
                '}';
    }
}
