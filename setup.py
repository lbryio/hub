import os
from scribe import __name__, __version__
from setuptools import setup, find_packages

BASE = os.path.dirname(__file__)
with open(os.path.join(BASE, 'README.md'), encoding='utf-8') as fh:
    long_description = fh.read()


setup(
    name=__name__,
    version=__version__,
    author="LBRY Inc.",
    author_email="hello@lbry.com",
    url="https://lbry.com",
    description="A decentralized media library and marketplace",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="lbry protocol electrum spv",
    license='MIT',
    python_requires='>=3.7',
    packages=find_packages(exclude=('tests',)),
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'scribe=scribe.cli:run_writer_forever',
            'scribe-hub=scribe.cli:run_server_forever',
            'scribe-elastic-sync=scribe.cli:run_es_sync_forever',
        ],
    },
    install_requires=[
        'aiohttp==3.5.4',
        'certifi>=2018.11.29',
        # 'colorama==0.3.7',
        # 'distro==1.4.0',
        'base58==1.0.0',
        'cffi==1.13.2',
        'cryptography==2.5',
        'protobuf==3.17.2',
        'msgpack==0.6.1',
        'prometheus_client==0.7.1',
        'ecdsa==0.13.3',
        'pyyaml==5.3.1',
        'prometheus_client==0.7.1',
        'coincurve==15.0.0',
        'pbkdf2==1.3',
        'attrs==18.2.0',
        'elasticsearch==7.10.1',
        'hachoir',
        'filetype==1.0.9',
        'grpcio==1.38.0'
        'lbry-rocksdb==0.8.2'
    ],
    extras_require={
        'lint': ['pylint==2.10.0'],
        'test': ['coverage'],
    },
    classifiers=[
        'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
        'Topic :: Internet',
        'Topic :: Software Development :: Testing',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        'Topic :: Utilities',
    ],
)
