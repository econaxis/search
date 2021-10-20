from json_generation_test import start_grpc_session, start_program, write
import random, subprocess, grpc, json
import time
from proto_generated import grpc_pb2
from proto_generated.grpc_pb2_grpc import *

EMPTY = grpc_pb2.Empty()

def test(stub: MainReplicatorStub):
    txn = stub.create_transaction(EMPTY)
    write(stub, txn, "test", '"a"')
    write(stub, txn, "test1", '"b"')

    txn1 = stub.create_transaction(EMPTY)
    write(stub, txn1, "test", '"c"')
    print(txn, txn1)

process = start_program()
service_stub = start_grpc_session()

test(service_stub)

process.kill()