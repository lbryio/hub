## Scribe

Scribe is a python library for building services that use the processed data from the [LBRY blockchain](https://github.com/lbryio/lbrycrd) in an ongoing manner. Scribe contains a set of three core executable services that are used together:
 * `scribe` ([scribe.blockchain.service](https://github.com/lbryio/scribe/tree/master/scribe/blockchain/service.py)) - maintains a [rocksdb](https://github.com/lbryio/lbry-rocksdb) database containing the LBRY blockchain.
 * `scribe-hub` ([scribe.hub.service](https://github.com/lbryio/scribe/tree/master/scribe/hub/service.py)) - an electrum server for thin-wallet clients (such as [lbry-sdk](https://github.com/lbryio/lbry-sdk)), provides an api for clients to use thin simple-payment-verification (spv) wallets and to resolve and search claims published to the LBRY blockchain.
 * `scribe-elastic-sync` ([scribe.elasticsearch.service](https://github.com/lbryio/scribe/tree/master/scribe/elasticsearch/service.py)) - a utility to maintain an elasticsearch database of metadata for claims in the LBRY blockchain

Features and overview of scribe as a python library:
 * Uses Python 3.7-3.9 (3.10 probably works but hasn't yet been tested)
 * An interface developers may implement in order to build their own applications able to receive up-to-date blockchain data in an ongoing manner ([scribe.service.BlockchainReaderService](https://github.com/lbryio/scribe/tree/master/scribe/service.py))
 * Protobuf schema for encoding and decoding metadata stored on the blockchain ([scribe.schema](https://github.com/lbryio/scribe/tree/master/scribe/schema))
 * [Rocksdb 6.25.3](https://github.com/lbryio/lbry-rocksdb/) based database containing the blockchain data ([scribe.db](https://github.com/lbryio/scribe/tree/master/scribe/db))
 * [A community driven performant trending algorithm](https://raw.githubusercontent.com/lbryio/scribe/master/scribe/elasticsearch/trending%20algorithm.pdf) for searching claims ([code](https://github.com/lbryio/scribe/blob/master/scribe/elasticsearch/fast_ar_trending.py))

## Installation

Scribe may be run from source, a binary, or a docker image.
Our [releases page](https://github.com/lbryio/scribe/releases) contains pre-built binaries of the latest release, pre-releases, and past releases for macOS and Debian-based Linux.
Prebuilt [docker images](https://hub.docker.com/r/lbry/scribe/latest-release) are also available.

### Prebuilt docker image

`docker pull lbry/scribe:latest-release`

### Build your own docker image

```
git clone https://github.com/lbryio/scribe.git
cd scribe
docker build -f ./docker/Dockerfile.scribe -t lbry/scribe:development .
```

### Install from source

Scribe has been tested with python 3.7-3.9. Higher versions probably work but have not yet been tested.

1. clone the scribe scribe
```
git clone https://github.com/lbryio/scribe.git
cd scribe
```
2. make a virtual env
```
python3.9 -m venv scribe-venv
```
3. from the virtual env, install scribe
```
source scribe-venv/bin/activate
pip install -e .
```

That completes the installation, now you should have the commands `scribe`, `scribe-elastic-sync` and `scribe-hub`

These can also optionally be run with `python -m scribe.blockchain`, `python -m scribe.elasticsearch`, and `python -m scribe.hub`

## Usage

### Requirements

Scribe needs elasticsearch and either the [lbrycrd](https://github.com/lbryio/lbrycrd) or [lbcd](https://github.com/lbryio/lbcd) blockchain daemon to be running.

With options for high performance, if you have 64gb of memory and 12 cores, everything can be run on the same machine. However, the recommended way is with elasticsearch on one instance with 8gb of memory and at least 4 cores dedicated to it and the blockchain daemon on another with 16gb of memory and at least 4 cores. Then the scribe hub services can be run their own instance with between 16 and 32gb of memory (depending on settings) and 8 cores. 

As of block 1147423 (4/21/22) the size of the scribe rocksdb database is 120GB and the size of the elasticsearch volume is 63GB.

### docker-compose
The recommended way to run a scribe hub is with docker.

If you have the resources to run all of the services on one machine (at least 300gb of fast storage, preferably nvme, 64gb of RAM, 12 fast cores), see [this](https://github.com/lbryio/scribe/blob/master/docker/docker-compose.yml) docker-compose example.

#### Cluster environment
For best performance the recommended setup uses three server instances, these can be rented VPSs, self hosted VMs (ideally not on one physical host unless the host is sufficiently powerful), or physical computers. One is a dedicated lbcd node, one an elasticsearch server, and the third runs the scribe services. With this configuration the lbcd and elasticsearch servers can be shared between multiple scribe hub servers - more on that later.
Server Requirements (space requirements are at least double what's needed so it's possible to copy snapshots into place or make snapshots):
  - lbcd: 2 cores, 8gb ram (slightly more may be required syncing from scratch, from a snapshot 8 is plenty), 150gb of NVMe storage
  - elasticsearch: 8 cores, 9gb of ram (8gb minimum given to ES), 150gb of SSD speed storage
  - scribe: 8 cores, 32gb of ram, 200gb of NVMe storage

All servers are assumed to be running ubuntu 20.04 with user named `lbry` with passwordless sudo and docker group permissions, ssh configured, ulimits set high, and docker + docker-compose installed. The server running elasticsearch should have swap disabled. The three servers need to be able to communicate with each other, they can be on a local network together or communicate over the internet. This guide will assume the three servers are on the internet.

##### Setting up the lbcd instance
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

##### Setting up the elasticsearch instance
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

##### Setting up the scribe hub instance
  - Log in (ssh) to the scribe instance and generate and print out a ssh key, this is needed to set up port forwards to the other two instances. Copy the output of the following:
```
ssh-keygen -q -t ed25519 -N '' -f ~/.ssh/id_ed25519 <<<y >/dev/null 2>&1
```
  - After copying the above key, log out of the scribe hub instance.

  - Log in to the elasticsearch instance add the copied key to `~/.ssh/authorized_keys` (see [this](https://stackoverflow.com/questions/6377009/adding-a-public-key-to-ssh-authorized-keys-does-not-log-me-in-automatically) if confused). Log out of the elasticsearch instance once done.
  - Log in to the lbcd instance and add the copied key to `~/.ssh/authorized_keys`, log out when done.
  - Log in to the scribe instance and copy the following to `/etc/systemd/system/es-tunnel.service`, replacing `lbry` with your user and `your-elastic-ip` with your elasticsearch instance ip.
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
  - Next, copy the following to `/etc/systemd/system/lbcd-tunnel.service` on the scribe instance, replacing `lbry` with your user and `your-lbcd-ip` with your lbcd instance ip.
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
  - Verify you can ssh in to the elasticsearch and lbcd instances from the scribe instance
  - Enable and start the ssh port forward services on the scribe instance
```
sudo systemctl enable es-tunnel.service
sudo systemctl enable lbcd-tunnel.service
sudo systemctl start es-tunnel.service
sudo systemctl start lbcd-tunnel.service
```
  - Build the scribe docker image on the scribe hub instance by running the following:
```
git clone https://github.com/lbryio/scribe.git
cd scribe
docker build -f ./docker/Dockerfile.scribe -t lbry/scribe:development .
```
  - Copy the following to `~/docker-compose.yml` on the scribe instance
```
version: "3"

volumes:
  lbry_rocksdb:

services:
  scribe:
    depends_on:
      - scribe_elastic_sync
    image: lbry/scribe:${SCRIBE_TAG:-development}
    restart: always
    network_mode: host
    volumes:
      - "lbry_rocksdb:/database"
    environment:
      - HUB_COMMAND=scribe
    command:
      - "--daemon_url=http://lbry:lbry@127.0.0.1:9245"
      - "--max_query_workers=2"
      - "--cache_all_tx_hashes"
  scribe_elastic_sync:
    image: lbry/scribe:${SCRIBE_TAG:-development}
    restart: always
    network_mode: host
    ports:
      - "127.0.0.1:19080:19080"  # elastic notifier port
    volumes:
      - "lbry_rocksdb:/database"
    environment:
      - HUB_COMMAND=scribe-elastic-sync
    command:
      - "--elastic_host=127.0.0.1"
      - "--elastic_port=9200"
      - "--max_query_workers=2"
      - "--filtering_channel_ids=770bd7ecba84fd2f7607fb15aedd2b172c2e153f 95e5db68a3101df19763f3a5182e4b12ba393ee8"
      - "--blocking_channel_ids=dd687b357950f6f271999971f43c785e8067c3a9 06871aa438032244202840ec59a469b303257cad b4a2528f436eca1bf3bf3e10ff3f98c57bd6c4c6"
  scribe_hub:
    depends_on:
      - scribe_elastic_sync
      - scribe
    image: lbry/scribe:${SCRIBE_TAG:-development}
    restart: always
    network_mode: host
    ports:
      - "50001:50001" # electrum rpc port and udp ping port
      - "2112:2112"   # comment out to disable prometheus metrics
    volumes:
      - "lbry_rocksdb:/database"
    environment:
      - HUB_COMMAND=scribe-hub
    command:
      - "--daemon_url=http://lbry:lbry@127.0.0.1:9245"
      - "--elastic_host=127.0.0.1"
      - "--elastic_port=9200"
      - "--max_query_workers=4"
      - "--host=0.0.0.0"
      - "--max_sessions=100000"
      - "--filtering_channel_ids=770bd7ecba84fd2f7607fb15aedd2b172c2e153f 95e5db68a3101df19763f3a5182e4b12ba393ee8"
      - "--blocking_channel_ids=dd687b357950f6f271999971f43c785e8067c3a9 06871aa438032244202840ec59a469b303257cad b4a2528f436eca1bf3bf3e10ff3f98c57bd6c4c6"
      - "--prometheus_port=2112"                # comment out to disable prometheus metrics
```
  - Start the scribe hub services by running `docker-compose up -d`
  - Check the status with `docker-compose logs -f --tail 100`

### From source

### Options

#### Common options across `scribe`, `scribe-hub`, and `scribe-elastic-sync`:
  - `--db_dir` (required) Path of the directory containing lbry-rocksdb, set from the environment with `DB_DIRECTORY`
  - `--daemon_url` (required for `scribe` and `scribe-hub`) URL for rpc from lbrycrd or lbcd<rpcuser>:<rpcpassword>@<lbrycrd rpc ip><lbrycrd rpc port>.
  - `--reorg_limit` Max reorg depth, defaults to 200, set from the environment with `REORG_LIMIT`.
  - `--chain` With blockchain to use - either `mainnet`, `testnet`, or `regtest` - set from the environment with `NET`
  - `--max_query_workers` Size of the thread pool, set from the environment with `MAX_QUERY_WORKERS`
  - `--cache_all_tx_hashes` If this flag is set, all tx hashes will be stored in memory. For `scribe`, this speeds up the rate it can apply blocks as well as process mempool. For `scribe-hub`, this will speed up syncing address histories. This setting will use 10+g of memory. It can be set from the environment with `CACHE_ALL_TX_HASHES=Yes`
  - `--cache_all_claim_txos` If this flag is set, all claim txos will be indexed in memory. Set from the environment with `CACHE_ALL_CLAIM_TXOS=Yes`
  - `--prometheus_port` If provided this port will be used to provide prometheus metrics, set from the environment with `PROMETHEUS_PORT`

#### Options for `scribe`
  - `--db_max_open_files` This setting translates into the max_open_files option given to rocksdb. A higher number will use more memory. Defaults to 64.

#### Options for `scribe-elastic-sync`
  - `--reindex` If this flag is set drop and rebuild the elasticsearch index.

#### Options for `scribe-hub`
  - `--host` Interface for server to listen on, use 0.0.0.0 to listen on the external interface. Can be set from the environment with `HOST`
  - `--tcp_port` Electrum TCP port to listen on for hub server. Can be set from the environment with `TCP_PORT`
  - `--udp_port` UDP port to listen on for hub server. Can be set from the environment with `UDP_PORT`
  - `--elastic_host` Hostname or ip address of the elasticsearch instance to connect to. Can be set from the environment with `ELASTIC_HOST`
  - `--elastic_port` Elasticsearch port to connect to. Can be set from the environment with `ELASTIC_PORT`
  - `--elastic_notifier_host` Elastic sync notifier host to connect to, defaults to localhost. Can be set from the environment with `ELASTIC_NOTIFIER_HOST`
  - `--elastic_notifier_port` Elastic sync notifier port to connect using. Can be set from the environment with `ELASTIC_NOTIFIER_PORT`
  - `--query_timeout_ms` Timeout for claim searches in elasticsearch in milliseconds. Can be set from the environment with `QUERY_TIMEOUT_MS`
  - `--blocking_channel_ids` Space separated list of channel claim ids used for blocking. Claims that are reposted by these channels can't be resolved or returned in search results. Can be set from the environment with `BLOCKING_CHANNEL_IDS`.
  - `--filtering_channel_ids` Space separated list of channel claim ids used for blocking. Claims that are reposted by these channels aren't returned in search results. Can be set from the environment with `FILTERING_CHANNEL_IDS`

## Contributing

Contributions to this project are welcome, encouraged, and compensated. For more details, please check [this](https://lbry.tech/contribute) link.

## License

This project is MIT licensed. For the full license, see [LICENSE](LICENSE).

## Security

We take security seriously. Please contact security@lbry.com regarding any security issues. [Our PGP key is here](https://lbry.com/faq/pgp-key) if you need it.

## Contact

The primary contact for this project is [@jackrobison](mailto:jackrobison@lbry.com).
