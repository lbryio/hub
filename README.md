## Scribe

`scribe` ([scribe.blockchain.service](https://github.com/lbryio/scribe/tree/master/scribe/blockchain/service.py)) - maintains a [rocksdb](https://github.com/lbryio/lbry-rocksdb) database containing the [LBRY blockchain](https://github.com/lbryio/lbrycrd) and provides an [interface](https://github.com/lbryio/scribe/tree/master/scribe/service.py) for python based services that utilize the blockchain data in an ongoing manner. Scribe includes two implementations of this interface:
 * `scribe-hub` ([scribe.hub.service](https://github.com/lbryio/scribe/tree/master/scribe/hub/service.py)) - an electrum server for thin-wallet clients (such as lbry-sdk), provides an api for clients to resolve and search claims published to the LBRY blockchain.
 * `scribe-elastic-sync` ([scribe.elasticsearch.service](https://github.com/lbryio/scribe/tree/master/scribe/elasticsearch/service.py)) - a utility to maintain an elasticsearch database of metadata for claims in the LBRY blockchain

Other features and overview of scribe:
 * Uses Python 3.7-3.9 (3.10 probably works but hasn't yet been tested)
 * Protobuf schema for encoding and decoding metadata stored on the blockchain ([scribe.schema](https://github.com/lbryio/scribe/tree/master/scribe/schema)).
 * [Rocksdb](https://github.com/lbryio/lbry-rocksdb/) based database containing the blockchain data ([scribe.db](https://github.com/lbryio/scribe/tree/master/scribe/db))


## Installation

Our [releases page](https://github.com/lbryio/scribe/releases) contains pre-built binaries of the latest release, pre-releases, and past releases for macOS and Debian-based Linux.
Prebuilt [docker images](https://hub.docker.com/r/lbry/scribe/latest-release) are also available.

## Usage

Scribe needs either the [lbrycrd](https://github.com/lbryio/lbrycrd) or [lbcd](https://github.com/lbryio/lbrycrd) blockchain daemon to be running.

As of block 1124663 (3/10/22) the size of the rocksdb database is 87GB and the size of the elasticsearch volume is 49GB.

To start scribe, run the following (providing your own args)

```
scribe --db_dir /your/db/path --daemon_url rpcuser:rpcpass@localhost:9245
```

## Running from source

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

## Trending

For a detailed explaination of the [trending algorithm](https://github.com/lbryio/scribe/blob/master/scribe/elasticsearch/fast_ar_trending.py) used for claim search, see [this](https://raw.githubusercontent.com/lbryio/scribe/master/scribe/elasticsearch/trending%20algorithm.pdf) paper

## Contributing

Contributions to this project are welcome, encouraged, and compensated. For more details, please check [this](https://lbry.tech/contribute) link.

## License

This project is MIT licensed. For the full license, see [LICENSE](LICENSE).

## Security

We take security seriously. Please contact security@lbry.com regarding any security issues. [Our PGP key is here](https://lbry.com/faq/pgp-key) if you need it.

## Contact

The primary contact for this project is [@jackrobison](mailto:jackrobison@lbry.com).
