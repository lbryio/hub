## Scribe

Scribe is a python library for building services that use the processed data from the [LBRY blockchain](https://github.com/lbryio/lbrycrd) in an ongoing manner. Scribe contains a set of three core executable services that are used together:
 * `scribe` ([scribe.blockchain.service](https://github.com/lbryio/scribe/tree/master/scribe/blockchain/service.py)) - maintains a [rocksdb](https://github.com/lbryio/lbry-rocksdb) database containing the LBRY blockchain.
 * `scribe-hub` ([scribe.hub.service](https://github.com/lbryio/scribe/tree/master/scribe/hub/service.py)) - an electrum server for thin-wallet clients (such as lbry-sdk), provides an api for clients to use thin simple-payment-verification (spv) wallets and to resolve and search claims published to the LBRY blockchain.
 * `scribe-elastic-sync` ([scribe.elasticsearch.service](https://github.com/lbryio/scribe/tree/master/scribe/elasticsearch/service.py)) - a utility to maintain an elasticsearch database of metadata for claims in the LBRY blockchain

Features and overview of scribe as a python library:
 * Uses Python 3.7-3.9 (3.10 probably works but hasn't yet been tested)
 * An interface developers may implement in order to build their own applications able to receive up-to-date blockchain data in an ongoing manner ([scribe.service.BlockchainReaderService](https://github.com/lbryio/scribe/tree/master/scribe/service.py))
 * Protobuf schema for encoding and decoding metadata stored on the blockchain ([scribe.schema](https://github.com/lbryio/scribe/tree/master/scribe/schema))
 * [Rocksdb](https://github.com/lbryio/lbry-rocksdb/) based database containing the blockchain data ([scribe.db](https://github.com/lbryio/scribe/tree/master/scribe/db))
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

## Usage

Scribe needs either the [lbrycrd](https://github.com/lbryio/lbrycrd) or [lbcd](https://github.com/lbryio/lbcd) blockchain daemon to be running.

As of block 1124663 (3/10/22) the size of the rocksdb database is 87GB and the size of the elasticsearch volume is 49GB.

### docker-compose

### From source

To start scribe, run the following (providing your own args)

```
scribe --db_dir /your/db/path --daemon_url rpcuser:rpcpass@localhost:9245
```

## Contributing

Contributions to this project are welcome, encouraged, and compensated. For more details, please check [this](https://lbry.tech/contribute) link.

## License

This project is MIT licensed. For the full license, see [LICENSE](LICENSE).

## Security

We take security seriously. Please contact security@lbry.com regarding any security issues. [Our PGP key is here](https://lbry.com/faq/pgp-key) if you need it.

## Contact

The primary contact for this project is [@jackrobison](mailto:jackrobison@lbry.com).
