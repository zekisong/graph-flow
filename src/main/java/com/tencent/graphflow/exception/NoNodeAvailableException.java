package com.tencent.graphflow.exception;

public class NoNodeAvailableException extends FlowRuntimeException {

    public NoNodeAvailableException(String message) {
        super(message, ErrorCode.NO_NODE_AVAILABLE_ERROR);
    }
}
