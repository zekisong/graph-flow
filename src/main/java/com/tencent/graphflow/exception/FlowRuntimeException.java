package com.tencent.graphflow.exception;

public class FlowRuntimeException extends RuntimeException {

    private int errorCode;

    public FlowRuntimeException(String message, int errorCode) {
        super(String.format("%s [error_code:%d]", message, errorCode));
        this.errorCode = errorCode;
    }

    public FlowRuntimeException(String message, Throwable cause) {
        super(String.format("%s [error_code:%d]", message,
                cause instanceof FlowException ? ((FlowException) cause).getErrorCode() : ErrorCode.INTERNAL_ERROR),
                cause);
    }

    public FlowRuntimeException(String message, Throwable cause, int errorCode) {
        super(String.format("%s [error_code:%d]", message, errorCode), cause);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public static void main(String[] args) throws FlowException {
        throw new FlowException("aaa", 100);
    }
}
