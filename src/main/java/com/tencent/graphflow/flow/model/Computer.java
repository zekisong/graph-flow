package com.tencent.graphflow.flow.model;

import java.io.Serializable;
import java.util.function.Function;

public interface Computer<T, R> extends Function<T, R>, Serializable {

}
