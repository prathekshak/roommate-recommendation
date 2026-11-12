# Deployment

The recommended deployment is Docker Compose on one Ubuntu EC2 instance. The
Compose network gives `node-a`, `node-b`, and `node-c` stable internal DNS names;
each node has its own persistent database volume.

Run the commands below from the project root. The root `.dockerignore` is kept
there because the Compose build context is the project root.

## Local Docker deployment

    cd /Users/pratheksha/SJSU/CS/DS-CS249/housing_platform
    docker compose -f deployment/compose.yaml build
    docker compose -f deployment/compose.yaml up -d
    docker compose -f deployment/compose.yaml ps

Open `http://127.0.0.1:8080` and check the API with:

    curl http://127.0.0.1:8201/health
    curl http://127.0.0.1:8201/raft/status

Exercise a failure:

    docker compose -f deployment/compose.yaml stop node-c
    curl http://127.0.0.1:8201/raft/status
    docker compose -f deployment/compose.yaml start node-c

Stop the stack with `docker compose -f deployment/compose.yaml down`. Named
volumes remain. Use `docker compose -f deployment/compose.yaml down -v` only
when you intentionally want to delete node data.

## EC2 deployment

1. Create an Ubuntu EC2 instance with Docker and Compose installed.
2. Copy this directory to the instance using Git or `scp`.
3. Run `docker compose -f deployment/compose.yaml up -d --build` from the
   project directory.
4. Allow TCP 8080 in the EC2 security group from your browser IP range and
   allow SSH 22 only from your own IP. Keep 8201-8203 private unless direct
   node access is needed for the course demo.
5. Open `http://<EC2_PUBLIC_IP>:8080`.

For a public deployment, add HTTPS/reverse-proxy termination in front of the
frontend. Do not expose Docker management ports or SQLite files. The separate
node volumes preserve the replicated-state demonstration.

## Updating

    docker compose -f deployment/compose.yaml up -d --build
    docker compose -f deployment/compose.yaml logs -f node-a
