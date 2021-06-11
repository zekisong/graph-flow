package com.graph.flow.server.rpc;

import com.google.protobuf.ByteString;
import com.graph.flow.exception.SerializeException;
import com.graph.flow.flow.model.FlowContext;
import com.graph.flow.proto.BootstrapFlowRequest;
import com.graph.flow.proto.ExecutorRpcServiceGrpc;
import com.graph.flow.proto.ExecutorRpcServiceGrpc.ExecutorRpcServiceBlockingStub;
import com.graph.flow.proto.ExecutorRpcServiceGrpc.ExecutorRpcServiceStub;
import com.graph.flow.proto.Node;
import com.graph.flow.proto.PipeRequest;
import com.graph.flow.serde.JavaSerDe;
import com.graph.flow.serde.SerDe;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RpcServiceClient {

    private static Map<Node, RpcServiceClient> CACHE = new ConcurrentHashMap<>();
    private SerDe serDe;
    private ExecutorRpcServiceBlockingStub client;
    private ExecutorRpcServiceStub asyncClient;

    public RpcServiceClient(Node target) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(target.getAddress(), target.getPort())
                .usePlaintext(true)
                .build();
        this.serDe = new JavaSerDe();
        this.client = ExecutorRpcServiceGrpc.newBlockingStub(channel);
        this.asyncClient = ExecutorRpcServiceGrpc.newStub(channel);
    }

    public boolean process(long flowId, int index, byte[] data) {
        PipeRequest request = PipeRequest.newBuilder()
                .setFlowId(flowId)
                .setIndex(index)
                .setData(ByteString.copyFrom(data))
                .build();
        return client.process(request).getSuccess();
    }

    public void processAsync(long flowId, int index, byte[] data) {
        PipeRequest request = PipeRequest.newBuilder()
                .setData(ByteString.copyFrom(data))
                .build();
        asyncClient.process(request, new StreamObserver() {
            @Override
            public void onNext(Object o) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });
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
