package com.graph.flow.hash;

import java.io.Serializable;

public interface HashStrategy<N> extends Serializable {

    int hash(N target);
}
