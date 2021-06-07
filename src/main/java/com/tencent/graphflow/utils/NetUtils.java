package com.tencent.graphflow.utils;

import com.tencent.graphflow.exception.FlowRuntimeException;
import java.io.IOException;
import java.net.ServerSocket;

public class NetUtils {

    public static int getFreeSocketPort() {
        int port;
        try {
            ServerSocket s = new ServerSocket(0);
            port = s.getLocalPort();
            s.close();
            return port;
        } catch (IOException e) {
            throw new FlowRuntimeException("aquire free port failed!", e);
        }
    }

}
