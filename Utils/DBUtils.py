from pymongo import MongoClient

from Utils.LoggerUtil import LoggerUtil


class DBUtils:
    def __init__(self):
        self.log = LoggerUtil(self.__class__.__name__).get()

    def get_client(self, address, port, username, password, auth_db, is_auth_enabled):
        try:
            if is_auth_enabled:
                client = MongoClient(
                    "mongodb://" + username + ":" + password + "@" + address + ":" + port + "/" + auth_db)
            else:
                client = MongoClient("mongodb://" + address + ":" + port + "/" + auth_db)
            return client
        except Exception as e:
            self.log.error("Error", e)
