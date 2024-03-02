# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import helloworld_service_pb2 as helloworld__service__pb2


class HelloWorldServiceStub(object):
    """The greeting service definition."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.SayHello = channel.unary_unary(
            '/HelloWorldService/SayHello',
            request_serializer=helloworld__service__pb2.HelloRequest.SerializeToString,
            response_deserializer=helloworld__service__pb2.HelloReply.FromString,
        )


class HelloWorldServiceServicer(object):
    """The greeting service definition."""

    def SayHello(self, request, context):
        """Sends a greeting"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_HelloWorldServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        'SayHello': grpc.unary_unary_rpc_method_handler(
            servicer.SayHello,
            request_deserializer=helloworld__service__pb2.HelloRequest.FromString,
            response_serializer=helloworld__service__pb2.HelloReply.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler('HelloWorldService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class HelloWorldService(object):
    """The greeting service definition."""

    @staticmethod
    def SayHello(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/HelloWorldService/SayHello',
            helloworld__service__pb2.HelloRequest.SerializeToString,
            helloworld__service__pb2.HelloReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
