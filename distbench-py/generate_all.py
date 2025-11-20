import yaml
import random
import argparse


def generate_random_topology(n, f):
    """
    Generates a random (2f+1)-connected topology with bidirectional edges.
    """
    min_deg = 2 * f + 1
    if min_deg >= n:
        raise ValueError("2f+1 must be < n for Dolev connectivity")

    neighbours = {f"n{i}": set() for i in range(n)}

    for i in range(n):
        node = f"n{i}"
        candidates = [f"n{j}" for j in range(n) if j != i]
        chosen = random.sample(candidates, min_deg)
        neighbours[node].update(chosen)

    for a, neighs in neighbours.items():
        for b in neighs:
            neighbours[b].add(a)

    return neighbours


def build_node_configs(n, f, neighbours, num_senders):
    sender_ids = random.sample(range(n), num_senders)
    payloads = ["msg_A", "msg_B", "msg_C", "msg_D", "msg_E"]

    configs = {}

    for i in range(n):
        node_id = f"n{i}"
        is_sender = i in sender_ids

        payload = payloads[sender_ids.index(i)] if is_sender else "hello"

        configs[node_id] = {
            "host": node_id,
            "port": 10000,
            "neighbours": sorted(list(neighbours[node_id])),
            "f": f,
            "is_sender": is_sender,
            "delay_min": 0.01,
            "delay_max": 0.05,
            "payload": payload,
            "behavior_mode": "HONEST",
        }

    return configs

def generate_docker_compose(configs, output_file):
    compose = {
        "version": "3.9",
        "services": {},
        "networks": {
            "dolev_network": {"driver": "bridge"}
        }
    }

    common = {
        "build": ".",
        "volumes": [
            "./configs:/app/configs",
            "./logs:/app/logs"
        ],
        "networks": ["dolev_network"],
        "environment": {
            "PYTHONUNBUFFERED": "1",
            "TERM": "dumb",
            "NO_COLOR": "1"
        }
    }

    for node in configs.keys():
        svc = common.copy()
        svc = dict(svc)

        svc["container_name"] = node
        svc["hostname"] = node
        svc["command"] = (
            f'sh -c "python -m distbench.main -c ${{CONFIG_FILE}} '
            f'-a dolev --mode network --id {node} -v > /app/logs/{node}.txt 2>&1"'
        )

        compose["services"][node] = svc

    with open(output_file, "w") as f:
        yaml.dump(compose, f, sort_keys=False)

    print(f"Docker compose written to: {output_file}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, required=True, help="Number of nodes")
    parser.add_argument("--f", type=int, required=True, help="Byzantine tolerance")
    parser.add_argument("--senders", type=int, default=3,
                        help="How many nodes are broadcasters")
    parser.add_argument("--topo-out", type=str, default="topology.yaml")
    parser.add_argument("--compose-out", type=str, default="docker-compose.generated.yaml")
    args = parser.parse_args()

    n, f = args.n, args.f

    print("Generating random topology...")
    neigh = generate_random_topology(n, f)

    print("Building node configs...")
    configs = build_node_configs(n, f, neigh, args.senders)

    print(f"Writing topology to {args.topo_out}")
    with open(args.topo_out, "w") as f:
        yaml.dump(configs, f, sort_keys=False)

    print("Generating docker compose...")
    generate_docker_compose(configs, args.compose_out)

    print(f"Topology: {args.topo_out}")
    print(f"Docker Compose: {args.compose_out}")

if __name__ == "__main__":
    main()
