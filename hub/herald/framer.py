from asyncio import Queue


class NewlineFramer:
    """A framer for a protocol where messages are separated by newlines."""

    # The default max_size value is motivated by JSONRPC, where a
    # normal request will be 250 bytes or less, and a reasonable
    # batch may contain 4000 requests.
    def __init__(self, max_size=250 * 4000):
        """max_size - an anti-DoS measure.  If, after processing an incoming
        message, buffered data would exceed max_size bytes, that
        buffered data is dropped entirely and the framer waits for a
        newline character to re-synchronize the stream.
        """
        self.max_size = max_size
        self.queue = Queue()
        self.received_bytes = self.queue.put_nowait
        self.synchronizing = False
        self.residual = b''

    def frame(self, message):
        return message + b'\n'

    async def receive_message(self):
        parts = []
        buffer_size = 0
        while True:
            part = self.residual
            self.residual = b''
            if not part:
                part = await self.queue.get()

            npos = part.find(b'\n')
            if npos == -1:
                parts.append(part)
                buffer_size += len(part)
                # Ignore over-sized messages; re-synchronize
                if buffer_size <= self.max_size:
                    continue
                self.synchronizing = True
                raise MemoryError(f'dropping message over {self.max_size:,d} '
                                  f'bytes and re-synchronizing')

            tail, self.residual = part[:npos], part[npos + 1:]
            if self.synchronizing:
                self.synchronizing = False
                return await self.receive_message()
            else:
                parts.append(tail)
                return b''.join(parts)
