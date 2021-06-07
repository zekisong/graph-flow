package com.tencent.graphflow.flow.model;

import java.io.Serializable;
import java.util.List;

public class Flow implements Serializable {

    private List<Operator> operators;

    public Flow(List<Operator> operators) {
        this.operators = operators;
    }

    public List<Operator> getOperators() {
        return operators;
    }
}
