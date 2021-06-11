package com.graph.flow.config;

public enum DataType {
    STRING((byte) 0),
    INTEGER((byte) 1),
    LONG((byte) 2),
    DOUBLE((byte) 3),
    FLOAT((byte) 4),
    TIMESTAMP((byte) 5);

    private byte value;

    DataType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static DataType valueOf(byte value) {
        switch (value) {
            case 0:
                return STRING;
            case 1:
                return INTEGER;
            case 2:
                return LONG;
            case 3:
                return DOUBLE;
            case 4:
                return FLOAT;
            case 5:
                return TIMESTAMP;
            default:
                throw new RuntimeException(String.format("DataType:%d not exits!", value));
        }
    }
}