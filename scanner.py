import os
import sys
import json
import curio
import socket
from levin.section import Section
from levin.bucket import Bucket
from levin.ctypes import *
from levin.constants import P2P_COMMANDS, LEVIN_SIGNATURE


port = 18080
host = "176.9.0.187"
first_peer = (host, port, None, None)
limit = 100
data_dir = "./data"


nodes_to_scan = set()
nodes_scanned = {
    "error": set(),
    "scanned": set(),
    "not_scanned_yet": set()
}
if not os.path.isdir(data_dir):
    os.mkdir(data_dir)
    nodes_to_scan.add(first_peer)
else:
    try:
        with open(f"{data_dir}/p2p_scan.json", "r") as f:
            prev_scan = json.load(f)
            nodes_to_scan = set()
            if len(prev_scan["error"]) > 0:
                for peer in prev_scan["error"]:
                    nodes_scanned["error"].add(tuple(peer))
            already_scanned = len(prev_scan["scanned"])
            if already_scanned > 0:
                for peer in prev_scan["scanned"]:
                    nodes_scanned["scanned"].add(tuple(peer))
            if len(prev_scan["not_scanned_yet"]) > 0:
                for peer in prev_scan["not_scanned_yet"]:
                    nodes_to_scan.add(tuple(peer))
            else:
                nodes_to_scan.add(first_peer)
    except FileNotFoundError:
        already_scanned = 0
        nodes_to_scan.add(first_peer)
        pass


def peer_finder(target:tuple) -> set:
    host, port, _, _ = target
    try:
        sock = socket.socket()
        sock.settimeout(10)
        sock.connect((host, port))
    except TimeoutError:
        sys.stderr.write(f"xx Connection to {host}:{port} timed out\n")
        sock.close()
        return None
    except ConnectionRefusedError:
        sys.stderr.write(f"xx Connection to {host}:{port} refused\n")
        sock.close()
        return None
    except:
        sys.stderr.write(f"-- Unable to connect to {host}:{port}\n")
        sock.close()
        return None

    bucket = Bucket.create_handshake_request()

    try:
        sock.send(bucket.header())
        sock.send(bucket.payload())
    except Exception as e:
        print(f">> Error while trying to sock.send() to {target}: {e}")
        sock.close()
        return None

    print(f">> sent packet '{P2P_COMMANDS[bucket.command]}' to {host}:{port}")

    buckets = []
    peers = []
    peers_set = set()

    while 1:
        try:
            buffer = sock.recv(8)
        except Exception as e:
            print(f"<< Error while trying to sock.recv() from {target}: {e}")
            sock.close()
            break
        if not buffer:
            sys.stderr.write(f"<< Invalid response from {target}: no buffer\n")
            break
        if not buffer.startswith(bytes(LEVIN_SIGNATURE)):
            sys.stderr.write(f"<< Invalid response from {target}: buffer does not start with Levin signature\n")
            break

        try:
            bucket = Bucket.from_buffer(signature=buffer, sock=sock)
            buckets.append(bucket)
        except TimeoutError:
            sys.stderr.write(f"<< Connection to {target} timed out while reading from buffer\n")
            break
        except Exception as e:
            sys.stderr.write(f"<< Error while reading from buffer from {target}: {e}\n")
            break

        if bucket.command == 1001:
            peers = bucket.get_peers() or []

            sock.close()
            break

    for peer in peers:
        try:
            new_node = (peer['ip'].ip, peer['port'].value, peer['pruning_seed'], peer['rpc_port'])
            peers_set.add(new_node)
        except:
            print(peer)
            # pass

    return peers_set


async def main():
    global nodes_to_scan
    global nodes_scanned
    i = 0
    threads_list = dict()
    while bool(nodes_to_scan):
        new_peers = set()
        async with curio.TaskGroup() as tg:
            while bool(nodes_to_scan):
                node = nodes_to_scan.pop()
                print(f"> spawning thread {i+1} for", node)
                try:
                    async with curio.timeout_after(5):
                        threads_list[i] = await tg.spawn_thread(peer_finder, node)
                except curio.TaskTimeout:
                    print(f"x Curio timeout while trying to contact {node}, cancelling task")
                    nodes_scanned["error"].add(node)
                    threads_list[i].cancel()
                    pass
                except Exception as e:
                    nodes_scanned["error"].add(node)
                    print(f"x Error while trying to contact {node}: {e}")
                    pass
                finally:
                    nodes_scanned["scanned"].add(node)
                i = i + 1
                if i >= limit: break
        for result_set in tg.results:
            if result_set is not None:
                new_peers.update(result_set)
        new_peers -= nodes_scanned["scanned"]  # Subtract nodes already scanned from the new nodes found
        nodes_to_scan.update(new_peers)
        if i >= limit:
            nodes_scanned["not_scanned_yet"].update(nodes_to_scan)
            break
    if i < limit:
        print(f"Scanned {i} nodes, then ran out (limit: {limit})")

try:
    curio.run(main())
finally:
    print("Done")

nodes_scanned_number = len(nodes_scanned["scanned"])
print("Nodes scanned: ", nodes_scanned_number - already_scanned)
print("Total nodes scanned: ", nodes_scanned_number)
# print(nodes_scanned["scanned"])
print("------------------")
print("Nodes to scan: ", len(nodes_scanned["not_scanned_yet"]))
# print(nodes_scanned["not_scanned_yet"])


# Save scan results to a json file
if len(nodes_scanned["scanned"]) != 0:  # Don't accidentally overwrite the file if we scanned 0 peers
    # Convert sets to lists before dumping as json
    nodes_scanned_json = dict()
    for k,v in nodes_scanned.items():
        nodes_scanned_json[k] = list(v)
    with open(data_dir + "/p2p_scan.json", "w") as pl_file:
        json.dump(nodes_scanned_json, pl_file)
else:
    print("0 peers scanned - NOT overwriting p2p_scan.json")
