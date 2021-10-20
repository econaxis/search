import random, subprocess, grpc, json
import time
from proto_generated import grpc_pb2
from proto_generated.grpc_pb2_grpc import *

# Assuming executable is located in search/rust/target/debug
DEFAULT_PATH = "../../target/debug/grpc-server"

# Uncomment when there's an existing GRPC server running (useful for attaching GDB)
# DEFAULT_PATH = "true"

# Server is configured to listen at this default host.
DEFAULT_URL = "127.0.0.1:50051"


def rand_str(len: int):
    return random.randbytes(len).hex()


def gen_random_dict(nesting_probability=0.3, max_depth=5):
    # Generates a random nested dict
    ret = {}
    for _ in range(0, random.randint(1, 20)):
        if random.random() < nesting_probability and max_depth > 0:
            ret[rand_str(5)] = gen_random_dict(nesting_probability, max_depth - 1)
        else:
            ret[rand_str(5)] = rand_str(random.randint(5, 30))

    return ret


def start_program():
    handle = subprocess.Popen([DEFAULT_PATH])

    # Wait for program to initialize and listen to host.
    time.sleep(0.5)
    return handle


def create_txn(stub: MainReplicatorStub):
    return stub.create_transaction(grpc_pb2.Empty())


def start_grpc_session():
    channel = grpc.insecure_channel(DEFAULT_URL)
    stub = MainReplicatorStub(channel)
    return stub


def test_dict(jsdict: dict, txn: grpc_pb2.LockDataRefId, path: str, stub: MainReplicatorStub):
    for key in jsdict:
        print(f"Testing {key}")
        read_request = grpc_pb2.ReadRequest(txn=txn, key=f"{path}/{key}")
        read_response = stub.read(read_request)
        print(f"Read {len(read_response.inner)} bytes: {read_response.inner[0:100]}...")
        response_dict = json.loads(read_response.inner)
        if response_dict != jsdict[key]:
            print(f"{read_response.inner} || {jsdict[key]}")
            raise RuntimeError("Read response doesn't match expected value")

        # If we have a nested object, then continue to check that nested object for correctness
        # Essentially, we're exhaustively reading every possible path to test for correctness.
        if type(jsdict[key]) == dict:
            test_dict(jsdict[key], txn, f"{path}/{key}", stub)


def test(stub: MainReplicatorStub):
    txn = create_txn(stub)
    print("Transaction: ", txn.id)

    data = json.dumps(gen_random_dict())
    js = grpc_pb2.Json(inner=data)

    path = "/" + random.randbytes(5).hex()
    write_request = grpc_pb2.JsonWriteRequest(txn=txn, path=path, value=js)

    _res = stub.write(write_request)

    read_request = grpc_pb2.ReadRequest(txn=txn, key=path)
    read_response = stub.read(read_request)
    print(f"Read {len(read_response.inner)} bytes")

    # Test each first-level key
    jsdict = json.loads(data)
    test_dict(jsdict, txn, path, stub)


def write(stub, txn, path, value):
    print("Writing ", value)
    write_req = grpc_pb2.JsonWriteRequest(txn=txn, path=path, value=grpc_pb2.Json(inner=value))
    return stub.write(write_req)


if __name__ == "__main__":
    process = start_program()
    service_stub = start_grpc_session()
    test(service_stub)

    process.kill()
