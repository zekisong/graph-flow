package com.graph.flow.exception;

public class SerializeException extends FlowException {

    public SerializeException(String message, Throwable cause) {
        super(message, cause, ErrorCode.SERIALIZE_ERROR);
    }
}
