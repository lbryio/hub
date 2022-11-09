## LBRY Hub

This repo provides a python library, `hub`, for building services that use the processed data from the [LBRY blockchain](https://github.com/lbryio/lbrycrd) in an ongoing manner. Hub contains a set of three core executable services that are used together:
 * `scribe` ([hub.scribe.service](https://github.com/lbryio/hub/tree/master/hub/service.py)) - maintains a [rocksdb](https://github.com/lbryio/lbry-rocksdb) database containing the LBRY blockchain.
 * `herald` ([hub.herald.service](https://github.com/lbryio/hub/tree/master/hub/herald/service.py)) - an electrum server for thin-wallet clients (such as [lbry-sdk](https://github.com/lbryio/lbry-sdk)), provides an api for clients to use thin simple-payment-verification (spv) wallets and to resolve and search claims published to the LBRY blockchain. A drop in replacement port of herald written in go - [herald.go](https://github.com/lbryio/herald.go) is currently being worked on.
 * `scribe-elastic-sync` ([hub.elastic_sync.service](https://github.com/lbryio/hub/tree/master/hub/elastic_sync/service.py)) - a utility to maintain an elasticsearch database of metadata for claims in the LBRY blockchain

![](https://raw.githubusercontent.com/lbryio/hub/master/docs/diagram.png)

Features and overview of `hub` as a python library:
 * Uses Python 3.7-3.9 (3.10 probably works but hasn't yet been tested)
 * An interface developers may implement in order to build their own applications able to receive up-to-date blockchain data in an ongoing manner ([hub.service.BlockchainReaderService](https://github.com/lbryio/hub/tree/master/hub/service.py))
 * Protobuf schema for encoding and decoding metadata stored on the blockchain ([hub.schema](https://github.com/lbryio/hub/tree/master/hub/schema))
 * [Rocksdb 6.25.3](https://github.com/lbryio/lbry-rocksdb/) based database containing the blockchain data ([hub.db](https://github.com/lbryio/hub/tree/master/hub/db))
 * [A community driven performant trending algorithm](https://raw.githubusercontent.com/lbryio/hub/master/docs/trending%20algorithm.pdf) for searching claims ([code](https://github.com/lbryio/hub/blob/master/hub/elastic_sync/fast_ar_trending.py))

## Installation

Scribe may be run from source, a binary, or a docker image.
Our [releases page](https://github.com/lbryio/hub/releases) contains pre-built binaries of the latest release, pre-releases, and past releases for macOS and Debian-based Linux.
Prebuilt [docker images](https://hub.docker.com/r/lbry/hub/tags) are also available.

### Prebuilt docker image

`docker pull lbry/hub:master`

### Build your own docker image

```
git clone https://github.com/lbryio/hub.git
cd hub
docker build -t lbry/hub:development .
```

### Install from source

Scribe has been tested with python 3.7-3.9. Higher versions probably work but have not yet been tested.

1. clone the scribe repo
```
git clone https://github.com/lbryio/hub.git
cd hub
```
2. make a virtual env
```
python3.9 -m venv hub-venv
```
3. from the virtual env, install scribe
```
source hub-venv/bin/activate
pip install -e .
```

That completes the installation, now you should have the commands `scribe`, `scribe-elastic-sync` and `herald`

These can also optionally be run with `python -m hub.scribe`, `python -m hub.elastic_sync`, and `python -m hub.herald`

## Usage

### Requirements

Scribe needs elasticsearch and either the [lbrycrd](https://github.com/lbryio/lbrycrd) or [lbcd](https://github.com/lbryio/lbcd) blockchain daemon to be running.

With options for high performance, if you have 64gb of memory and 12 cores, everything can be run on the same machine. However, the recommended way is with elasticsearch on one instance with 8gb of memory and at least 4 cores dedicated to it and the blockchain daemon on another with 16gb of memory and at least 4 cores. Then the scribe hub services can be run their own instance with between 16 and 32gb of memory (depending on settings) and 8 cores. 

As of block 1147423 (4/21/22) the size of the scribe rocksdb database is 120GB and the size of the elasticsearch volume is 63GB.

### docker-compose
The recommended way to run a scribe hub is with docker. See [this guide](https://github.com/lbryio/hub/blob/master/docs/cluster_guide.md) for instructions.

If you have the resources to run all of the services on one machine (at least 300gb of fast storage, preferably nvme, 64gb of RAM, 12 fast cores), see [this](https://github.com/lbryio/hub/blob/master/docs/docker_examples/docker-compose.yml) docker-compose example.

### From source

### Options

#### Content blocking and filtering

For various reasons it may be desirable to block or filtering content from claim search and resolve results, [here](https://github.com/lbryio/hub/blob/master/docs/blocking.md) are instructions for how to configure and use this feature as well as information about the recommended defaults.

#### Common options across `scribe`, `herald`, and `scribe-elastic-sync`:
  - `--db_dir` (required) Path of the directory containing lbry-rocksdb, set from the environment with `DB_DIRECTORY`
  - `--daemon_url` (required for `scribe` and `herald`) URL for rpc from lbrycrd or lbcd<rpcuser>:<rpcpassword>@<lbrycrd rpc ip><lbrycrd rpc port>.
  - `--reorg_limit` Max reorg depth, defaults to 200, set from the environment with `REORG_LIMIT`.
  - `--chain` With blockchain to use - either `mainnet`, `testnet`, or `regtest` - set from the environment with `NET`
  - `--max_query_workers` Size of the thread pool, set from the environment with `MAX_QUERY_WORKERS`
  - `--cache_all_tx_hashes` If this flag is set, all tx hashes will be stored in memory. For `scribe`, this speeds up the rate it can apply blocks as well as process mempool. For `herald`, this will speed up syncing address histories. This setting will use 10+g of memory. It can be set from the environment with `CACHE_ALL_TX_HASHES=Yes`
  - `--cache_all_claim_txos` If this flag is set, all claim txos will be indexed in memory. Set from the environment with `CACHE_ALL_CLAIM_TXOS=Yes`
  - `--prometheus_port` If provided this port will be used to provide prometheus metrics, set from the environment with `PROMETHEUS_PORT`

#### Options for `scribe`
  - `--db_max_open_files` This setting translates into the max_open_files option given to rocksdb. A higher number will use more memory. Defaults to 64.
  - `--address_history_cache_size` The count of items in the address history cache used for processing blocks and mempool updates. A higher number will use more memory, shouldn't ever need to be higher than 10000. Defaults to 1000.
  - `--index_address_statuses` Maintain an index of the statuses of address transaction histories, this makes handling notifications for transactions in a block uniformly fast at the expense of more time to process new blocks and somewhat more disk space (~10gb as of block 1161417).

#### Options for `scribe-elastic-sync`
  - `--reindex` If this flag is set drop and rebuild the elasticsearch index.

#### Options for `herald`
  - `--host` Interface for server to listen on, use 0.0.0.0 to listen on the external interface. Can be set from the environment with `HOST`
  - `--tcp_port` Electrum TCP port to listen on for hub server. Can be set from the environment with `TCP_PORT`
  - `--udp_port` UDP port to listen on for hub server. Can be set from the environment with `UDP_PORT`
  - `--elastic_services` Comma separated list of items in the format `elastic_host:elastic_port/notifier_host:notifier_port`. Can be set from the environment with `ELASTIC_SERVICES`
  - `--query_timeout_ms` Timeout for claim searches in elasticsearch in milliseconds. Can be set from the environment with `QUERY_TIMEOUT_MS`
  - `--blocking_channel_ids` Space separated list of channel claim ids used for blocking. Claims that are reposted by these channels can't be resolved or returned in search results. Can be set from the environment with `BLOCKING_CHANNEL_IDS`.
  - `--filtering_channel_ids` Space separated list of channel claim ids used for blocking. Claims that are reposted by these channels aren't returned in search results. Can be set from the environment with `FILTERING_CHANNEL_IDS`
  - `--index_address_statuses` Use the address history status index, this makes handling notifications for transactions in a block uniformly fast (must be turned on in `scribe` too).

## Contributing

Contributions to this project are welcome, encouraged, and compensated. For more details, please check [this](https://lbry.tech/contribute) link.

## License

This project is MIT licensed. For the full license, see [LICENSE](LICENSE).

## Security

We take security seriously. Please contact security@lbry.com regarding any security issues. [Our PGP key is here](https://lbry.com/faq/pgp-key) if you need it.

## Contact

The primary contact for this project is [@jackrobison](mailto:jackrobison@lbry.com).
