package com.graph.flow.server.rpc;

import com.graph.flow.exception.SerializeException;
import com.graph.flow.flow.runtime.Batch;
import com.graph.flow.proto.BootstrapFlowRequest;
import com.graph.flow.proto.BootstrapFlowResponse;
import com.graph.flow.proto.ExecutorRpcServiceGrpc.ExecutorRpcServiceImplBase;
import com.graph.flow.proto.PipeRequest;
import com.graph.flow.proto.PipeResponse;
import com.graph.flow.server.NodeManager;
import io.grpc.stub.StreamObserver;

public class RpcService extends ExecutorRpcServiceImplBase {

    private NodeManager manager;

    public RpcService(NodeManager manager) {
        this.manager = manager;
    }

    @Override
    public void process(PipeRequest request, StreamObserver<PipeResponse> responseObserver) {
        Batch batch = Batch.decode(request.getData().toByteArray());
        manager.getFlowEngine().process(request.getFlowId(), request.getIndex(), batch);
        responseObserver.onNext(PipeResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void bootstrap(BootstrapFlowRequest request, StreamObserver<BootstrapFlowResponse> responseObserver) {
        BootstrapFlowResponse response;
        try {
            manager.getFlowEngine().bootstrap(request.getData());
            response = BootstrapFlowResponse.newBuilder().setSuccess(true).build();
        } catch (SerializeException e) {
            response = BootstrapFlowResponse.newBuilder().setSuccess(false).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
