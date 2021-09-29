import random, json, requests, subprocess, grpc, grpc_pb2_grpc, grpc_pb2
import time
DEFAULT_PATH = "../../target/debug/grpc-server"
DEFAULT_URL = "127.0.0.1:50051"


def rand_str(len):
    return random.randbytes(len).hex()


def gen_random_dict(nesting_probability=0.4, max_depth=5):
    # Generates a random nested dict

    ret = {}
    for _ in range(0, random.randint(1, 10)):
        if random.random() < nesting_probability and max_depth > 0:
            ret[rand_str(5)] = gen_random_dict(nesting_probability, max_depth - 1)
        else:
            ret[rand_str(5)] = rand_str(random.randint(5, 30))

    return ret


def start_program():
    handle =  subprocess.Popen([DEFAULT_PATH])
    time.sleep(1)
    return handle



txn_counter = 2


def create_txn():
    global txn_counter
    txn_counter += 1
    return grpc_pb2.LockDataRefId(id=txn_counter + 1)


def start_grpc_session():
    channel = grpc.insecure_channel(DEFAULT_URL)
    stub = grpc_pb2_grpc.ReplicatorStub(channel)
    return stub


def test(stub: grpc_pb2_grpc.ReplicatorStub):
    txn = create_txn()
    result = stub.new_with_time(txn)
    print(result)
    result = stub.serve_write



process = start_program()
stub = start_grpc_session()
test(stub)

process.kill()
