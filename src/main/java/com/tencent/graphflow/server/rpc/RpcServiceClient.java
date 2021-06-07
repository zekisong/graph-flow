package com.tencent.graphflow.server.rpc;

import com.google.protobuf.ByteString;
import com.tencent.graphflow.exception.SerializeException;
import com.tencent.graphflow.flow.model.FlowContext;
import com.tencent.graphflow.serde.JavaSerDe;
import com.tencent.graphflow.serde.SerDe;
import com.tencent.tdf.proto.BootstrapFlowRequest;
import com.tencent.tdf.proto.ExecutorRpcServiceGrpc;
import com.tencent.tdf.proto.ExecutorRpcServiceGrpc.ExecutorRpcServiceBlockingStub;
import com.tencent.tdf.proto.Node;
import com.tencent.tdf.proto.PipeRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RpcServiceClient {

    private static Map<Node, RpcServiceClient> CACHE = new ConcurrentHashMap<>();
    private SerDe serDe;
    private ExecutorRpcServiceBlockingStub client;

    public RpcServiceClient(Node target) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(target.getAddress(), target.getPort())
                .usePlaintext(true)
                .build();
        this.serDe = new JavaSerDe();
        this.client = ExecutorRpcServiceGrpc.newBlockingStub(channel);
    }

    public boolean process(byte[] data) {
        PipeRequest request = PipeRequest.newBuilder()
                .setData(ByteString.copyFrom(data))
                .build();
        return client.process(request).getSuccess();
    }

    public boolean process() {
        PipeRequest request = PipeRequest.newBuilder()
                .build();
        return client.process(request).getSuccess();
    }

    public boolean bootstrap(FlowContext context) throws SerializeException {
        byte[] data = serDe.serialize(context);
        BootstrapFlowRequest request = BootstrapFlowRequest.newBuilder()
                .setData(ByteString.copyFrom(data))
                .build();
        return client.bootstrap(request).getSuccess();
    }

    public static RpcServiceClient getClient(Node target) {
        return CACHE.computeIfAbsent(target, v -> new RpcServiceClient(v));
    }
}
