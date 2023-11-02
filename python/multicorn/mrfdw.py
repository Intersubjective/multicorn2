from multicorn import ForeignDataWrapper, Qual
import pynng
import msgpack

DEFAULT_URL = f'tcp://127.0.0.1:10234'

class MeritrankFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super().__init__(options, columns)

        self.url = options.get("url", DEFAULT_URL)

    def execute(self, quals, columns, sortkeys=None):
        quals_tuple = tuple((q.field_name, q.operator, q.value) for q in quals)
        msg = msgpack.dumps((quals_tuple, columns))

        with pynng.Req0() as sock:
            sock.dial(self.url)
            sock.send_msg(msg)
            resp = sock.recv_msg()
            for row in msgpack.loads(resp.bytes):
                yield row
