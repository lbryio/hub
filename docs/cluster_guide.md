## Cluster environment guide

For best performance the recommended setup uses three server instances, these can be rented VPSs, self hosted VMs (ideally not on one physical host unless the host is sufficiently powerful), or physical computers. One is a dedicated lbcd node, one an elasticsearch server, and the third runs the hub services (scribe, herald, and scribe-elastic-sync). With this configuration the lbcd and elasticsearch servers can be shared between multiple herald servers - more on that later.
Server Requirements (space requirements are at least double what's needed so it's possible to copy snapshots into place or make snapshots):
  - lbcd: 2 cores, 8gb ram (slightly more may be required syncing from scratch, from a snapshot 8 is plenty), 150gb of NVMe storage
  - elasticsearch: 8 cores, 9gb of ram (8gb minimum given to ES), 150gb of SSD speed storage
  - hub: 8 cores, 32gb of ram, 200gb of NVMe storage

All servers are assumed to be running ubuntu 20.04 with user named `lbry` with passwordless sudo and docker group permissions, ssh configured, ulimits set high, and docker + docker-compose installed. The server running elasticsearch should have swap disabled. The three servers need to be able to communicate with each other, they can be on a local network together or communicate over the internet. This guide will assume the three servers are on the internet.

### Setting up the lbcd instance
Log in to the lbcd instance and perform the following steps:
  - Build the lbcd docker image by running
```
git clone https://github.com/lbryio/lbcd.git
cd lbcd
docker build . -t lbry/lbcd:latest
```
  - Copy the following to `~/docker-compose.yml`
```
version: "3"

volumes:
  lbcd:

services:
  lbcd:
    image: lbry/lbcd:latest
    restart: always
    network_mode: host
    command:
      - "--notls"
      - "--rpcuser=lbry"
      - "--rpcpass=lbry"
      - "--rpclisten=127.0.0.1"
    volumes:
      - "lbcd:/root/.lbcd"
    ports:
      - "127.0.0.1:9245:9245"
      - "9246:9246"  # p2p port
```
  - Start lbcd by running `docker-compose up -d`
  - Check the progress with `docker-compose logs -f --tail 100`

### Setting up the elasticsearch instance
Log in to the elasticsearch instance and perform the following steps:
  - Copy the following to `~/docker-compose.yml`
```
version: "3"

volumes:
  es01:

services:
  es01:
    image: docker.elastic.co/elasticsearch/elasticsearch:7.16.0
    container_name: es01
    environment:
      - node.name=es01
      - discovery.type=single-node
      - indices.query.bool.max_clause_count=8192
      - bootstrap.memory_lock=true
      - "ES_JAVA_OPTS=-Dlog4j2.formatMsgNoLookups=true -Xms8g -Xmx8g"  # no more than 32, remember to disable swap
    ulimits:
      memlock:
        soft: -1
        hard: -1
    volumes:
      - "es01:/usr/share/elasticsearch/data"
    ports:
      - "127.0.0.1:9200:9200"
```
  - Start elasticsearch by running `docker-compose up -d`
  - Check the status with `docker-compose logs -f --tail 100`

### Setting up the hub instance
  - Log in (ssh) to the hub instance and generate and print out a ssh key, this is needed to set up port forwards to the other two instances. Copy the output of the following:
```
ssh-keygen -q -t ed25519 -N '' -f ~/.ssh/id_ed25519 <<<y >/dev/null 2>&1
```
  - After copying the above key, log out of the hub instance.

  - Log in to the elasticsearch instance add the copied key to `~/.ssh/authorized_keys` (see [this](https://stackoverflow.com/questions/6377009/adding-a-public-key-to-ssh-authorized-keys-does-not-log-me-in-automatically) if confused). Log out of the elasticsearch instance once done.
  - Log in to the lbcd instance and add the copied key to `~/.ssh/authorized_keys`, log out when done.
  - Log in to the hub instance and copy the following to `/etc/systemd/system/es-tunnel.service`, replacing `lbry` with your user and `your-elastic-ip` with your elasticsearch instance ip.
```
[Unit]
Description=Persistent SSH Tunnel for ES
After=network.target

[Service]
Restart=on-failure
RestartSec=5
ExecStart=/usr/bin/ssh -NTC -o ServerAliveInterval=60 -o ExitOnForwardFailure=yes -L 127.0.0.1:9200:127.0.0.1:9200 lbry@your-elastic-ip
User=lbry
Group=lbry

[Install]
WantedBy=multi-user.target
```
  - Next, copy the following to `/etc/systemd/system/lbcd-tunnel.service` on the hub instance, replacing `lbry` with your user and `your-lbcd-ip` with your lbcd instance ip.
```
[Unit]
Description=Persistent SSH Tunnel for lbcd
After=network.target

[Service]
Restart=on-failure
RestartSec=5
ExecStart=/usr/bin/ssh -NTC -o ServerAliveInterval=60 -o ExitOnForwardFailure=yes -L 127.0.0.1:9245:127.0.0.1:9245 lbry@your-lbcd-ip
User=lbry
Group=lbry

[Install]
WantedBy=multi-user.target
```
  - Verify you can ssh in to the elasticsearch and lbcd instances from the hub instance
  - Enable and start the ssh port forward services on the hub instance
```
sudo systemctl enable es-tunnel.service
sudo systemctl enable lbcd-tunnel.service
sudo systemctl start es-tunnel.service
sudo systemctl start lbcd-tunnel.service
```
  - Build the hub docker image on the hub instance by running the following:
```
git clone https://github.com/lbryio/hub.git
cd hub
docker build -t lbry/hub:development .
```
  - Copy the following to `~/docker-compose.yml` on the hub instance
```
version: "3"

volumes:
  lbry_rocksdb:

services:
  scribe:
    depends_on:
      - scribe_elastic_sync
    image: lbry/hub:${SCRIBE_TAG:-development}
    restart: always
    network_mode: host
    volumes:
      - "lbry_rocksdb:/database"
    environment:
      - HUB_COMMAND=scribe
      - SNAPSHOT_URL=https://snapshots.lbry.com/hub/lbry-rocksdb.zip
    command:
      - "--daemon_url=http://lbry:lbry@127.0.0.1:9245"
      - "--max_query_workers=2"
      - "--cache_all_tx_hashes"
      - "--index_address_statuses"
  scribe_elastic_sync:
    image: lbry/hub:${SCRIBE_TAG:-development}
    restart: always
    network_mode: host
    ports:
      - "127.0.0.1:19080:19080"  # elastic notifier port
    volumes:
      - "lbry_rocksdb:/database"
    environment:
      - HUB_COMMAND=scribe-elastic-sync
      - FILTERING_CHANNEL_IDS=770bd7ecba84fd2f7607fb15aedd2b172c2e153f 95e5db68a3101df19763f3a5182e4b12ba393ee8
      - BLOCKING_CHANNEL_IDS=dd687b357950f6f271999971f43c785e8067c3a9 06871aa438032244202840ec59a469b303257cad b4a2528f436eca1bf3bf3e10ff3f98c57bd6c4c6
    command:
      - "--elastic_host=127.0.0.1"
      - "--elastic_port=9200"
      - "--max_query_workers=2"
  herald:
    depends_on:
      - scribe_elastic_sync
      - scribe
    image: lbry/hub:${SCRIBE_TAG:-development}
    restart: always
    network_mode: host
    ports:
      - "50001:50001" # electrum rpc port and udp ping port
      - "2112:2112"   # comment out to disable prometheus metrics
    volumes:
      - "lbry_rocksdb:/database"
    environment:
      - HUB_COMMAND=herald
      - FILTERING_CHANNEL_IDS=770bd7ecba84fd2f7607fb15aedd2b172c2e153f 95e5db68a3101df19763f3a5182e4b12ba393ee8
      - BLOCKING_CHANNEL_IDS=dd687b357950f6f271999971f43c785e8067c3a9 06871aa438032244202840ec59a469b303257cad b4a2528f436eca1bf3bf3e10ff3f98c57bd6c4c6
    command:
      - "--index_address_statuses"
      - "--daemon_url=http://lbry:lbry@127.0.0.1:9245"
      - "--elastic_host=127.0.0.1"
      - "--elastic_port=9200"
      - "--max_query_workers=4"
      - "--host=0.0.0.0"
      - "--max_sessions=100000"
      - "--prometheus_port=2112"                # comment out to disable prometheus metrics
```
  - Start the hub services by running `docker-compose up -d`
  - Check the status with `docker-compose logs -f --tail 100`
