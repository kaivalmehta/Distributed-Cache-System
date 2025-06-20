# datastore.py

class DataStore:
    def __init__(self):
        self.store = {
            "hello": "world",
            "code": "it559",
            "dis": "sys"
        }

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value

