package com.tencent.graphflow.server.rpc;

import com.tencent.graphflow.exception.SerializeException;
import com.tencent.graphflow.flow.runtime.Batch;
import com.tencent.graphflow.server.NodeManager;
import com.tencent.tdf.proto.BootstrapFlowRequest;
import com.tencent.tdf.proto.BootstrapFlowResponse;
import com.tencent.tdf.proto.ExecutorRpcServiceGrpc.ExecutorRpcServiceImplBase;
import com.tencent.tdf.proto.PipeRequest;
import com.tencent.tdf.proto.PipeResponse;
import io.grpc.stub.StreamObserver;
import java.util.List;

public class RpcService extends ExecutorRpcServiceImplBase {

    private NodeManager manager;

    public RpcService(NodeManager manager) {
        this.manager = manager;
    }

    @Override
    public void process(PipeRequest request, StreamObserver<PipeResponse> responseObserver) {
        List batch = Batch.decode(request.getData().toByteArray());
        manager.getFlowEngine().process(batch);
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
