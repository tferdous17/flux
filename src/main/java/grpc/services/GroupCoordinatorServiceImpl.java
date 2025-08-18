package grpc.services;

import io.grpc.stub.StreamObserver;
import proto.GroupCoordinatorServiceGrpc;
import proto.JoinGroupRequest;
import proto.JoinGroupResponse;

public class GroupCoordinatorServiceImpl extends GroupCoordinatorServiceGrpc.GroupCoordinatorServiceImplBase {

    @Override
    public void joinGroup(JoinGroupRequest req, StreamObserver<JoinGroupResponse> responseObserver) {
        // figure out leader


        // compute the intersection of all protocols from followers

        // create partition mapping


    }
}
