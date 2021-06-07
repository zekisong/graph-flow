package com.tencent.graphflow.exception;

public class ContextTimeOutException extends FlowException {

    public ContextTimeOutException(String message) {
        super(message, ErrorCode.FLOW_CONTEXT_TIMEOUT_ERROR);
    }
}
