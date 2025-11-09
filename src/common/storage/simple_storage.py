class SimpleStorage:
    def __init__(self):
        self._data = {}

    def set(self, key, value):
        self._data[key] = value

    def get(self, key, default=None):
        return self._data.get(key, default)

    def delete(self, key):
        if key in self._data:
            del self._data[key]

    def is_request_executed(self, request_id):
        if not request_id:
            return False
        return f"req_{request_id}" in self._data

    def mark_request_executed(self, request_id, response_data):
        if request_id:
            self._data[f"req_{request_id}"] = response_data

    def get_cached_response(self, request_id):
        return self._data.get(f"req_{request_id}")

    def isReady(self):
        return True

    def _isLeader(self):
        return True
