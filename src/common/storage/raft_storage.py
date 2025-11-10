from pysyncobj import SyncObj, replicated

class RaftStorage(SyncObj):
    def __init__(self, self_address, other_server_addresses):
        super().__init__(self_address, other_server_addresses)
        self._data = {}

    @replicated
    def set(self, key, value):
        self._data[key] = value

    def get(self, key, default=None):
        return self._data.get(key, default)

    @replicated
    def delete(self, key):
        if key in self._data:
            del self._data[key]

    def is_request_executed(self, request_id):
        if not request_id:
            return False
        return f"req_{request_id}" in self._data

    @replicated
    def mark_request_executed(self, request_id, response_data):
        if request_id:
            self._data[f"req_{request_id}"] = response_data

    def get_cached_response(self, request_id):
        return self._data.get(f"req_{request_id}")

