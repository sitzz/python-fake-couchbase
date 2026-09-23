class RawResult:
    """Stand-in for the SDK's private C result type (``pycbc_core.result``).

    The SDK's public ``Result`` classes only ever read ``raw_result`` (and, on
    4.1.x, call ``err()``), so a plain object is enough. Unlike the C type, whose
    ``raw_result`` became readonly in SDK 4.6, this one is writable on every SDK.
    """

    def __init__(self, raw_result=None):
        self.raw_result = raw_result

    def err(self):
        return None

    def __repr__(self):
        return f"RawResult({self.raw_result!r})"
