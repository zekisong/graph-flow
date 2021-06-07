package com.tencent.graphflow.server.rpc;

import com.tencent.graphflow.LifeCycle;
import com.tencent.graphflow.exception.ErrorCode;
import com.tencent.graphflow.exception.FlowRuntimeException;
import com.tencent.tdf.proto.Node;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;

public class RpcServer implements LifeCycle {

    private ServerBuilder builder;
    private Server server;

    public RpcServer(Node node) {
        this.builder = NettyServerBuilder.forAddress(new InetSocketAddress(node.getAddress(), node.getPort()));
    }

    public void addService(BindableService service) {
        builder.addService(service);
    }

    @Override
    public void start() {
        try {
            server = builder.build();
            server.start();
        } catch (IOException e) {
            throw new FlowRuntimeException("RPC SERVER START FAILED!", e, ErrorCode.RPC_SERVER_START_FAILED);
        }
    }

    @Override
    public void stop() {
        server.shutdown();
    }
}
