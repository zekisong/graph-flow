package com.graph.flow.utils;


import com.graph.flow.proto.Node;

public class NodeUtils {

    public static String toString(Node node) {
        return node.getAddress() + ":" + node.getPort();
    }

    public static Node fromString(String addr) {
        String[] items = addr.split(":");
        if (items.length < 2) {
            return null;
        }
        return Node.newBuilder().setAddress(items[0]).setPort(Integer.valueOf(items[1])).build();
    }
}
