import itertools
import json
import typing
import asyncio
from asyncio import Event
from functools import partial
from numbers import Number
from scribe.common import RPCError, CodeMessageError
from scribe.hub.common import Notification, Request, Response, Batch, ProtocolError


class JSONRPC:
    """Abstract base class that interprets and constructs JSON RPC messages."""

    # Error codes.  See http://www.jsonrpc.org/specification
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_ARGS = -32602
    INTERNAL_ERROR = -32603
    QUERY_TIMEOUT = -32000

    # Codes specific to this library
    ERROR_CODE_UNAVAILABLE = -100

    # Can be overridden by derived classes
    allow_batches = True

    @classmethod
    def _message_id(cls, message, require_id):
        """Validate the message is a dictionary and return its ID.

        Raise an error if the message is invalid or the ID is of an
        invalid type.  If it has no ID, raise an error if require_id
        is True, otherwise return None.
        """
        raise NotImplementedError

    @classmethod
    def _validate_message(cls, message):
        """Validate other parts of the message other than those
        done in _message_id."""
        pass

    @classmethod
    def _request_args(cls, request):
        """Validate the existence and type of the arguments passed
        in the request dictionary."""
        raise NotImplementedError

    @classmethod
    def _process_request(cls, payload):
        request_id = None
        try:
            request_id = cls._message_id(payload, False)
            cls._validate_message(payload)
            method = payload.get('method')
            if request_id is None:
                item = Notification(method, cls._request_args(payload))
            else:
                item = Request(method, cls._request_args(payload))
            return item, request_id
        except ProtocolError as error:
            code, message = error.code, error.message
        raise cls._error(code, message, True, request_id)

    @classmethod
    def _process_response(cls, payload):
        request_id = None
        try:
            request_id = cls._message_id(payload, True)
            cls._validate_message(payload)
            return Response(cls.response_value(payload)), request_id
        except ProtocolError as error:
            code, message = error.code, error.message
        raise cls._error(code, message, False, request_id)

    @classmethod
    def _message_to_payload(cls, message):
        """Returns a Python object or a ProtocolError."""
        try:
            return json.loads(message.decode())
        except UnicodeDecodeError:
            message = 'messages must be encoded in UTF-8'
        except json.JSONDecodeError:
            message = 'invalid JSON'
        raise cls._error(cls.PARSE_ERROR, message, True, None)

    @classmethod
    def _error(cls, code, message, send, msg_id):
        error = ProtocolError(code, message)
        if send:
            error.error_message = cls.response_message(error, msg_id)
        else:
            error.response_msg_id = msg_id
        return error

    #
    # External API
    #

    @classmethod
    def message_to_item(cls, message):
        """Translate an unframed received message and return an
        (item, request_id) pair.

        The item can be a Request, Notification, Response or a list.

        A JSON RPC error response is returned as an RPCError inside a
        Response object.

        If a Batch is returned, request_id is an iterable of request
        ids, one per batch member.

        If the message violates the protocol in some way a
        ProtocolError is returned, except if the message was
        determined to be a response, in which case the ProtocolError
        is placed inside a Response object.  This is so that client
        code can mark a request as having been responded to even if
        the response was bad.

        raises: ProtocolError
        """
        payload = cls._message_to_payload(message)
        if isinstance(payload, dict):
            if 'method' in payload:
                return cls._process_request(payload)
            else:
                return cls._process_response(payload)
        elif isinstance(payload, list) and cls.allow_batches:
            if not payload:
                raise cls._error(JSONRPC.INVALID_REQUEST, 'batch is empty',
                                 True, None)
            return payload, None
        raise cls._error(cls.INVALID_REQUEST,
                         'request object must be a dictionary', True, None)

    # Message formation
    @classmethod
    def request_message(cls, item, request_id):
        """Convert an RPCRequest item to a message."""
        assert isinstance(item, Request)
        return cls.encode_payload(cls.request_payload(item, request_id))

    @classmethod
    def notification_message(cls, item):
        """Convert an RPCRequest item to a message."""
        assert isinstance(item, Notification)
        return cls.encode_payload(cls.request_payload(item, None))

    @classmethod
    def response_message(cls, result, request_id):
        """Convert a response result (or RPCError) to a message."""
        if isinstance(result, CodeMessageError):
            payload = cls.error_payload(result, request_id)
        else:
            payload = cls.response_payload(result, request_id)
        return cls.encode_payload(payload)

    @classmethod
    def batch_message(cls, batch, request_ids):
        """Convert a request Batch to a message."""
        assert isinstance(batch, Batch)
        if not cls.allow_batches:
            raise ProtocolError.invalid_request(
                'protocol does not permit batches')
        id_iter = iter(request_ids)
        rm = cls.request_message
        nm = cls.notification_message
        parts = (rm(request, next(id_iter)) if isinstance(request, Request)
                 else nm(request) for request in batch)
        return cls.batch_message_from_parts(parts)

    @classmethod
    def batch_message_from_parts(cls, messages):
        """Convert messages, one per batch item, into a batch message.  At
        least one message must be passed.
        """
        # Comma-separate the messages and wrap the lot in square brackets
        middle = b', '.join(messages)
        if not middle:
            raise ProtocolError.empty_batch()
        return b''.join([b'[', middle, b']'])

    @classmethod
    def encode_payload(cls, payload):
        """Encode a Python object as JSON and convert it to bytes."""
        try:
            return json.dumps(payload).encode()
        except TypeError:
            msg = f'JSON payload encoding error: {payload}'
            raise ProtocolError(cls.INTERNAL_ERROR, msg) from None


class JSONRPCv1(JSONRPC):
    """JSON RPC version 1.0."""

    allow_batches = False

    @classmethod
    def _message_id(cls, message, require_id):
        # JSONv1 requires an ID always, but without constraint on its type
        # No need to test for a dictionary here as we don't handle batches.
        if 'id' not in message:
            raise ProtocolError.invalid_request('request has no "id"')
        return message['id']

    @classmethod
    def _request_args(cls, request):
        args = request.get('params')
        if not isinstance(args, list):
            raise ProtocolError.invalid_args(
                f'invalid request arguments: {args}')
        return args

    @classmethod
    def _best_effort_error(cls, error):
        # Do our best to interpret the error
        code = cls.ERROR_CODE_UNAVAILABLE
        message = 'no error message provided'
        if isinstance(error, str):
            message = error
        elif isinstance(error, int):
            code = error
        elif isinstance(error, dict):
            if isinstance(error.get('message'), str):
                message = error['message']
            if isinstance(error.get('code'), int):
                code = error['code']

        return RPCError(code, message)

    @classmethod
    def response_value(cls, payload):
        if 'result' not in payload or 'error' not in payload:
            raise ProtocolError.invalid_request(
                'response must contain both "result" and "error"')

        result = payload['result']
        error = payload['error']
        if error is None:
            return result   # It seems None can be a valid result
        if result is not None:
            raise ProtocolError.invalid_request(
                'response has a "result" and an "error"')

        return cls._best_effort_error(error)

    @classmethod
    def request_payload(cls, request, request_id):
        """JSON v1 request (or notification) payload."""
        if isinstance(request.args, dict):
            raise ProtocolError.invalid_args(
                'JSONRPCv1 does not support named arguments')
        return {
            'method': request.method,
            'params': request.args,
            'id': request_id
        }

    @classmethod
    def response_payload(cls, result, request_id):
        """JSON v1 response payload."""
        return {
            'result': result,
            'error': None,
            'id': request_id
        }

    @classmethod
    def error_payload(cls, error, request_id):
        return {
            'result': None,
            'error': {'code': error.code, 'message': error.message},
            'id': request_id
        }


class JSONRPCv2(JSONRPC):
    """JSON RPC version 2.0."""

    @classmethod
    def _message_id(cls, message, require_id):
        if not isinstance(message, dict):
            raise ProtocolError.invalid_request(
                'request object must be a dictionary')
        if 'id' in message:
            request_id = message['id']
            if not isinstance(request_id, (Number, str, type(None))):
                raise ProtocolError.invalid_request(
                    f'invalid "id": {request_id}')
            return request_id
        else:
            if require_id:
                raise ProtocolError.invalid_request('request has no "id"')
            return None

    @classmethod
    def _validate_message(cls, message):
        if message.get('jsonrpc') != '2.0':
            raise ProtocolError.invalid_request('"jsonrpc" is not "2.0"')

    @classmethod
    def _request_args(cls, request):
        args = request.get('params', [])
        if not isinstance(args, (dict, list)):
            raise ProtocolError.invalid_args(
                f'invalid request arguments: {args}')
        return args

    @classmethod
    def response_value(cls, payload):
        if 'result' in payload:
            if 'error' in payload:
                raise ProtocolError.invalid_request(
                    'response contains both "result" and "error"')
            return payload['result']

        if 'error' not in payload:
            raise ProtocolError.invalid_request(
                'response contains neither "result" nor "error"')

        # Return an RPCError object
        error = payload['error']
        if isinstance(error, dict):
            code = error.get('code')
            message = error.get('message')
            if isinstance(code, int) and isinstance(message, str):
                return RPCError(code, message)

        raise ProtocolError.invalid_request(
            f'ill-formed response error object: {error}')

    @classmethod
    def request_payload(cls, request, request_id):
        """JSON v2 request (or notification) payload."""
        payload = {
            'jsonrpc': '2.0',
            'method': request.method,
        }
        # A notification?
        if request_id is not None:
            payload['id'] = request_id
        # Preserve empty dicts as missing params is read as an array
        if request.args or request.args == {}:
            payload['params'] = request.args
        return payload

    @classmethod
    def response_payload(cls, result, request_id):
        """JSON v2 response payload."""
        return {
            'jsonrpc': '2.0',
            'result': result,
            'id': request_id
        }

    @classmethod
    def error_payload(cls, error, request_id):
        return {
            'jsonrpc': '2.0',
            'error': {'code': error.code, 'message': error.message},
            'id': request_id
        }


class JSONRPCLoose(JSONRPC):
    """A relaxed version of JSON RPC."""

    # Don't be so loose we accept any old message ID
    _message_id = JSONRPCv2._message_id
    _validate_message = JSONRPC._validate_message
    _request_args = JSONRPCv2._request_args
    # Outoing messages are JSONRPCv2 so we give the other side the
    # best chance to assume / detect JSONRPCv2 as default protocol.
    error_payload = JSONRPCv2.error_payload
    request_payload = JSONRPCv2.request_payload
    response_payload = JSONRPCv2.response_payload

    @classmethod
    def response_value(cls, payload):
        # Return result, unless it is None and there is an error
        if payload.get('error') is not None:
            if payload.get('result') is not None:
                raise ProtocolError.invalid_request(
                    'response contains both "result" and "error"')
            return JSONRPCv1._best_effort_error(payload['error'])

        if 'result' not in payload:
            raise ProtocolError.invalid_request(
                'response contains neither "result" nor "error"')

        # Can be None
        return payload['result']


class JSONRPCAutoDetect(JSONRPCv2):

    @classmethod
    def message_to_item(cls, message):
        return cls.detect_protocol(message), None

    @classmethod
    def detect_protocol(cls, message):
        """Attempt to detect the protocol from the message."""
        main = cls._message_to_payload(message)

        def protocol_for_payload(payload):
            if not isinstance(payload, dict):
                return JSONRPCLoose   # Will error
            # Obey an explicit "jsonrpc"
            version = payload.get('jsonrpc')
            if version == '2.0':
                return JSONRPCv2
            if version == '1.0':
                return JSONRPCv1

            # Now to decide between JSONRPCLoose and JSONRPCv1 if possible
            if 'result' in payload and 'error' in payload:
                return JSONRPCv1
            return JSONRPCLoose

        if isinstance(main, list):
            parts = {protocol_for_payload(payload) for payload in main}
            # If all same protocol, return it
            if len(parts) == 1:
                return parts.pop()
            # If strict protocol detected, return it, preferring JSONRPCv2.
            # This means a batch of JSONRPCv1 will fail
            for protocol in (JSONRPCv2, JSONRPCv1):
                if protocol in parts:
                    return protocol
            # Will error if no parts
            return JSONRPCLoose

        return protocol_for_payload(main)


class JSONRPCConnection:
    """Maintains state of a JSON RPC connection, in particular
    encapsulating the handling of request IDs.

    protocol - the JSON RPC protocol to follow
    max_response_size - responses over this size send an error response
        instead.
    """

    _id_counter = itertools.count()

    def __init__(self, protocol):
        self._protocol = protocol
        # Sent Requests and Batches that have not received a response.
        # The key is its request ID; for a batch it is sorted tuple
        # of request IDs
        self._requests: typing.Dict[str, typing.Tuple[Request, Event]] = {}
        # A public attribute intended to be settable dynamically
        self.max_response_size = 0

    def _oversized_response_message(self, request_id):
        text = f'response too large (over {self.max_response_size:,d} bytes'
        error = RPCError.invalid_request(text)
        return self._protocol.response_message(error, request_id)

    def _receive_response(self, result, request_id):
        if request_id not in self._requests:
            if request_id is None and isinstance(result, RPCError):
                message = f'diagnostic error received: {result}'
            else:
                message = f'response to unsent request (ID: {request_id})'
            raise ProtocolError.invalid_request(message) from None
        request, event = self._requests.pop(request_id)
        event.result = result
        event.set()
        return []

    def _receive_request_batch(self, payloads):
        def item_send_result(request_id, result):
            nonlocal size
            part = protocol.response_message(result, request_id)
            size += len(part) + 2
            if size > self.max_response_size > 0:
                part = self._oversized_response_message(request_id)
            parts.append(part)
            if len(parts) == count:
                return protocol.batch_message_from_parts(parts)
            return None

        parts = []
        items = []
        size = 0
        count = 0
        protocol = self._protocol
        for payload in payloads:
            try:
                item, request_id = protocol._process_request(payload)
                items.append(item)
                if isinstance(item, Request):
                    count += 1
                    item.send_result = partial(item_send_result, request_id)
            except ProtocolError as error:
                count += 1
                parts.append(error.error_message)

        if not items and parts:
            protocol_error = ProtocolError(0, "")
            protocol_error.error_message = protocol.batch_message_from_parts(parts)
            raise protocol_error
        return items

    def _receive_response_batch(self, payloads):
        request_ids = []
        results = []
        for payload in payloads:
            # Let ProtocolError exceptions through
            item, request_id = self._protocol._process_response(payload)
            request_ids.append(request_id)
            results.append(item.result)

        ordered = sorted(zip(request_ids, results), key=lambda t: t[0])
        ordered_ids, ordered_results = zip(*ordered)
        if ordered_ids not in self._requests:
            raise ProtocolError.invalid_request('response to unsent batch')
        request_batch, event = self._requests.pop(ordered_ids)
        event.result = ordered_results
        event.set()
        return []

    def _send_result(self, request_id, result):
        message = self._protocol.response_message(result, request_id)
        if len(message) > self.max_response_size > 0:
            message = self._oversized_response_message(request_id)
        return message

    def _event(self, request, request_id):
        event = Event()
        self._requests[request_id] = (request, event)
        return event

    #
    # External API
    #
    def send_request(self, request: Request) -> typing.Tuple[bytes, Event]:
        """Send a Request.  Return a (message, event) pair.

        The message is an unframed message to send over the network.
        Wait on the event for the response; which will be in the
        "result" attribute.

        Raises: ProtocolError if the request violates the protocol
        in some way..
        """
        request_id = next(self._id_counter)
        message = self._protocol.request_message(request, request_id)
        return message, self._event(request, request_id)

    def send_notification(self, notification):
        return self._protocol.notification_message(notification)

    def send_batch(self, batch):
        ids = tuple(next(self._id_counter)
                    for request in batch if isinstance(request, Request))
        message = self._protocol.batch_message(batch, ids)
        event = self._event(batch, ids) if ids else None
        return message, event

    def receive_message(self, message):
        """Call with an unframed message received from the network.

        Raises: ProtocolError if the message violates the protocol in
        some way.  However, if it happened in a response that can be
        paired with a request, the ProtocolError is instead set in the
        result attribute of the send_request() that caused the error.
        """
        try:
            item, request_id = self._protocol.message_to_item(message)
        except ProtocolError as e:
            if e.response_msg_id is not id:
                return self._receive_response(e, e.response_msg_id)
            raise

        if isinstance(item, Request):
            item.send_result = partial(self._send_result, request_id)
            return [item]
        if isinstance(item, Notification):
            return [item]
        if isinstance(item, Response):
            return self._receive_response(item.result, request_id)
        if isinstance(item, list):
            if all(isinstance(payload, dict)
                   and ('result' in payload or 'error' in payload)
                   for payload in item):
                return self._receive_response_batch(item)
            else:
                return self._receive_request_batch(item)
        else:
            # Protocol auto-detection hack
            assert issubclass(item, JSONRPC)
            self._protocol = item
            return self.receive_message(message)

    def raise_pending_requests(self, exception):
        exception = exception or asyncio.TimeoutError()
        for request, event in self._requests.values():
            event.result = exception
            event.set()
        self._requests.clear()

    def pending_requests(self):
        """All sent requests that have not received a response."""
        return [request for request, event in self._requests.values()]
