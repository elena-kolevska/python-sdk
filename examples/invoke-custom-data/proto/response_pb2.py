# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: response.proto
"""Generated protocol buffer code."""

from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x0eresponse.proto"B\n\x0e\x43ustomResponse\x12\x11\n\tisSuccess\x18\x01 \x01(\x08\x12\x0c\n\x04\x63ode\x18\x02 \x01(\x05\x12\x0f\n\x07message\x18\x03 \x01(\tb\x06proto3'
)


_CUSTOMRESPONSE = DESCRIPTOR.message_types_by_name['CustomResponse']
CustomResponse = _reflection.GeneratedProtocolMessageType(
    'CustomResponse',
    (_message.Message,),
    {
        'DESCRIPTOR': _CUSTOMRESPONSE,
        '__module__': 'response_pb2',
        # @@protoc_insertion_point(class_scope:CustomResponse)
    },
)
_sym_db.RegisterMessage(CustomResponse)

if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _CUSTOMRESPONSE._serialized_start = 18
    _CUSTOMRESPONSE._serialized_end = 84
# @@protoc_insertion_point(module_scope)
