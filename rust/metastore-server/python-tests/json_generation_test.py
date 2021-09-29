import random, json, requests, subprocess, grpc, grpc_pb2_grpc, grpc_pb2, json
import time

DEFAULT_PATH = "../../target/debug/grpc-server"
DEFAULT_PATH = "ls"

DEFAULT_URL = "127.0.0.1:50051"


def rand_str(len):
    return random.randbytes(len).hex()


def gen_random_dict(nesting_probability=0.1, max_depth=50):
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
    time.sleep(1)
    return handle


def create_txn(stub: grpc_pb2_grpc.MainReplicatorStub):
    return stub.create_transaction(grpc_pb2.Empty())


def start_grpc_session():
    channel = grpc.insecure_channel(DEFAULT_URL)
    stub = grpc_pb2_grpc.MainReplicatorStub(channel)
    return stub


def test(stub: grpc_pb2_grpc.MainReplicatorStub):
    txn = create_txn(stub)
    print("Transaction: ", txn.id)

    data = json.dumps(gen_random_dict())
    js = grpc_pb2.Json(inner=data)

    path = "/" + random.randbytes(5).hex()
    write_request = grpc_pb2.JsonWriteRequest(txn=txn, path=path, value=js)

    res = stub.write(write_request)

    read_request = grpc_pb2.ReadRequest(txn=txn, key=path)
    read_response = stub.read(read_request)
    print("Read: ", read_response)


process = start_program()
stub = start_grpc_session()
test(stub)

process.kill()
