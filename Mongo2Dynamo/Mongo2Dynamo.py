from json import load
from urllib.parse import quote

from boto3 import resource

from Utils.ConfigUtil import ConfigUtil
from Utils.DBUtils import DBUtils
from Utils.LoggerUtil import LoggerUtil


class ParseMongo:
    def __init__(self, chunk_size=1000):
        self.db_utils = DBUtils()
        self.log = LoggerUtil(self.__class__.__name__).get()
        self.config = ConfigUtil.get_config_instance()
        self.chunk_size = chunk_size

    def get_collection(self):
        username = self.config['mongo']['username']
        pwd = self.config['mongo']['password']
        password = quote(pwd)
        address = self.config['mongo']['address']
        port = self.config['mongo']['port']
        auth_db = self.config['mongo']['auth_db']
        is_auth_enabled = self.config['mongo']['is_auth_enabled']

        client = self.db_utils.get_client(address=address, port=port, auth_db=auth_db,
                                          is_auth_enabled=is_auth_enabled,
                                          username=username, password=password)
        db_name = self.config['mongo']['db_name']
        cuckoo_db = client[db_name]

        phylogeny_collection_name = self.config['mongo']['phylogeny_collection']

        phylogeny_collection = cuckoo_db[phylogeny_collection_name]

        return phylogeny_collection

    @staticmethod
    def get_batch(phylogeny_collection, keys):
        query = [
            {"$match": {"key": {"$in": keys}}},
            {"$addFields": {"__order": {"$indexOfArray": [keys, "$key"]}}},
            {"$sort": {"__order": 1}}
        ]
        cursor = phylogeny_collection.find(query)
        return cursor

    def init_resources(self):
        list_of_keys_path = self.config["data"]["list_of_keys"]
        phylogeny_collection = self.get_collection()
        list_of_keys = load(open(list_of_keys_path + "/" + "list_of_keys.json", 'r'))
        return phylogeny_collection, list_of_keys


class Mongo2Dynamo:
    def __init__(self):
        self.log = LoggerUtil(self.__class__.__name__).get()
        self.config = ConfigUtil.get_config_instance()
        self.mongo = ParseMongo(chunk_size=1000)

    @staticmethod
    def init_resources(resource_name, resource_region, table_name):
        dynamodb = resource(resource_name, resource_region)
        table = dynamodb.Table(table_name)
        return table

    @staticmethod
    def parse(doc):
        return {k: v for k, v in doc.items() if k != "_id"}

    def parse_and_put_batch(self, table, batch):
        """
        For each batch of mongo documents, parse them and insert in the dynamo required format.
        :return:
        """
        with table.batch_writer() as dynamo_batch:
            try:
                for doc in batch:
                    item = self.parse(doc)
                    dynamo_batch.put_item(Item=item)
            except Exception as e:
                self.log.error(F"Batch Write Error :{e}")

    def populate(self, table, phylogeny_collection, list_of_keys, chunk_size):
        """
        Populates the table with the values parsed from Mongo
        :return:
        """
        counter = 0
        while counter < len(list_of_keys):
            self.log.info(F"Working on Iter : #{counter / chunk_size}")
            if counter + chunk_size < len(list_of_keys):
                p_keys = list_of_keys[counter: counter + chunk_size]
            else:
                p_keys = list_of_keys[counter:]
            try:
                batch = self.mongo.get_batch(phylogeny_collection, p_keys)
                self.parse_and_put_batch(table, batch)
            except Exception as e:
                self.log.error(F"Error : {e}")
            counter += chunk_size

    def main(self):
        """
        Driver program
        :return:
        """
        resource_name = 'dynamodb'
        resource_region = 'us-east-1'
        table_name = "MalwarePhylogeny"
        chunk_size = 1000
        table = self.init_resources(resource_name=resource_name,
                                    resource_region=resource_region,
                                    table_name=table_name)
        phylogeny_collection, list_of_keys = self.mongo.init_resources()
        self.populate(table=table, phylogeny_collection=phylogeny_collection,
                      list_of_keys=list_of_keys, chunk_size=chunk_size)


if __name__ == '__main__':
    m2d = Mongo2Dynamo()
    m2d.main()
