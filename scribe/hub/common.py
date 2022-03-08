import logging
import itertools
import time
import json
import typing
import asyncio
import inspect
from asyncio import Event
from collections import namedtuple
from functools import partial, lru_cache
from numbers import Number
from asyncio import Queue
from scribe.common import RPCError, CodeMessageError


HISTOGRAM_BUCKETS = (
    .005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 20.0, 30.0, 60.0, float('inf')
)


SignatureInfo = namedtuple('SignatureInfo', 'min_args max_args '
                           'required_names other_names')

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_ARGS = -32602
INTERNAL_ERROR = -32603
QUERY_TIMEOUT = -32000


@lru_cache(256)
def signature_info(func):
    params = inspect.signature(func).parameters
    min_args = max_args = 0
    required_names = []
    other_names = []
    no_names = False
    for p in params.values():
        if p.kind == p.POSITIONAL_OR_KEYWORD:
            max_args += 1
            if p.default is p.empty:
                min_args += 1
                required_names.append(p.name)
            else:
                other_names.append(p.name)
        elif p.kind == p.KEYWORD_ONLY:
            other_names.append(p.name)
        elif p.kind == p.VAR_POSITIONAL:
            max_args = None
        elif p.kind == p.VAR_KEYWORD:
            other_names = any
        elif p.kind == p.POSITIONAL_ONLY:
            max_args += 1
            if p.default is p.empty:
                min_args += 1
            no_names = True

    if no_names:
        other_names = None

    return SignatureInfo(min_args, max_args, required_names, other_names)


class BatchError(Exception):

    def __init__(self, request):
        self.request = request   # BatchRequest object


class BatchRequest:
    """Used to build a batch request to send to the server.  Stores
    the

    Attributes batch and results are initially None.

    Adding an invalid request or notification immediately raises a
    ProtocolError.

    On exiting the with clause, it will:

    1) create a Batch object for the requests in the order they were
       added.  If the batch is empty this raises a ProtocolError.

    2) set the "batch" attribute to be that batch

    3) send the batch request and wait for a response

    4) raise a ProtocolError if the protocol was violated by the
       server.  Currently this only happens if it gave more than one
       response to any request

    5) otherwise there is precisely one response to each Request.  Set
       the "results" attribute to the tuple of results; the responses
       are ordered to match the Requests in the batch.  Notifications
       do not get a response.

    6) if raise_errors is True and any individual response was a JSON
       RPC error response, or violated the protocol in some way, a
       BatchError exception is raised.  Otherwise the caller can be
       certain each request returned a standard result.
    """

    def __init__(self, session, raise_errors):
        self._session = session
        self._raise_errors = raise_errors
        self._requests = []
        self.batch = None
        self.results = None

    def add_request(self, method, args=()):
        self._requests.append(Request(method, args))

    def add_notification(self, method, args=()):
        self._requests.append(Notification(method, args))

    def __len__(self):
        return len(self._requests)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.batch = Batch(self._requests)
            message, event = self._session.connection.send_batch(self.batch)
            await self._session._send_message(message)
            await event.wait()
            self.results = event.result
            if self._raise_errors:
                if any(isinstance(item, Exception) for item in event.result):
                    raise BatchError(self)


class SingleRequest:
    __slots__ = ('method', 'args')

    def __init__(self, method, args):
        if not isinstance(method, str):
            raise ProtocolError(METHOD_NOT_FOUND,
                                'method must be a string')
        if not isinstance(args, (list, tuple, dict)):
            raise ProtocolError.invalid_args('request arguments must be a '
                                             'list or a dictionary')
        self.args = args
        self.method = method

    def __repr__(self):
        return f'{self.__class__.__name__}({self.method!r}, {self.args!r})'

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.method == other.method and self.args == other.args)


class Request(SingleRequest):
    def send_result(self, response):
        return None


class Notification(SingleRequest):
    pass


class Batch:
    __slots__ = ('items', )

    def __init__(self, items):
        if not isinstance(items, (list, tuple)):
            raise ProtocolError.invalid_request('items must be a list')
        if not items:
            raise ProtocolError.empty_batch()
        if not (all(isinstance(item, SingleRequest) for item in items) or
                all(isinstance(item, Response) for item in items)):
            raise ProtocolError.invalid_request('batch must be homogeneous')
        self.items = items

    def __len__(self):
        return len(self.items)

    def __getitem__(self, item):
        return self.items[item]

    def __iter__(self):
        return iter(self.items)

    def __repr__(self):
        return f'Batch({len(self.items)} items)'


class Response:
    __slots__ = ('result', )

    def __init__(self, result):
        # Type checking happens when converting to a message
        self.result = result


class ProtocolError(CodeMessageError):
    def __init__(self, code, message):
        super().__init__(code, message)
        # If not None send this unframed message over the network
        self.error_message = None
        # If the error was in a JSON response message; its message ID.
        # Since None can be a response message ID, "id" means the
        # error was not sent in a JSON response
        self.response_msg_id = id
