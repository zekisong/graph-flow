package com.tencent.graphflow.exception;

public class FlowException extends Exception {

    private int errorCode;

    public FlowException(String message, int errorCode) {
        super(String.format("%s [error_code:%d]", message, errorCode));
        this.errorCode = errorCode;
    }

    public FlowException(String message, Throwable cause, int errorCode) {
        super(String.format("%s [error_code:%d]", message, errorCode), cause);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
