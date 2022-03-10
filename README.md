Scribe maintains a [rocksdb](https://github.com/lbryio/lbry-rocksdb) database containing the [LBRY blockchain](https://github.com/lbryio/lbrycrd) and provides an interface for python based services that utilize the blockchain data in an ongoing manner. Scribe includes implementations of this interface to provide an electrum server for thin-wallet clients (such as lbry-sdk) and to maintain an elasticsearch database of metadata for claims in the LBRY blockchain.

 * Uses Python 3.7-3.8
 * Protobuf schema for encoding and decoding metadata stored on the blockchain ([scribe.schema](https://github.com/lbryio/scribe/tree/master/scribe/schema)).
 * Blockchain processor that maintains an up to date rocksdb database ([scribe.blockchain](https://github.com/lbryio/scribe/tree/master/scribe/blockchain))
 * Rocksdb based database containing the blockchain data ([scribe.db](https://github.com/lbryio/scribe/tree/master/scribe/db))
 * Interface for python services to implement in order for them maintain a read only view of the blockchain data ([scribe.reader.interface](https://github.com/lbryio/scribe/tree/master/scribe/reader/interface.py))
 * Electrum based server for thin-wallet clients like lbry-sdk ([scribe.reader.hub_server](https://github.com/lbryio/scribe/tree/master/scribe/reader/hub_server.py))
 * Elasticsearch sync utility to index all the claim metadata in the blockchain into an easily searchable form ([scribe.reader.elastic_sync](https://github.com/lbryio/scribe/tree/master/scribe/reader/elastic_sync.py))


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

Installing from source is also relatively painless. Full instructions are in [INSTALL.md](INSTALL.md)

## Contributing

Contributions to this project are welcome, encouraged, and compensated. For more details, please check [this](https://lbry.tech/contribute) link.

## License

This project is MIT licensed. For the full license, see [LICENSE](LICENSE).

## Security

We take security seriously. Please contact security@lbry.com regarding any security issues. [Our PGP key is here](https://lbry.com/faq/pgp-key) if you need it.

## Contact

The primary contact for this project is [@jackrobison](mailto:jackrobison@lbry.com).
