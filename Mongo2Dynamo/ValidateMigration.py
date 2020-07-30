from json import load, dump

from boto3 import resource

from Utils.ConfigUtil import ConfigUtil
from Utils.LoggerUtil import LoggerUtil


class ValidateMigration:
    def __init__(self):
        self.log = LoggerUtil(self.__class__.__name__).get()
        self.config = ConfigUtil.get_config_instance()

    def get_dynamo_keys(self, table, list_of_keys):
        fault_keys = list()
        for vs_key in list_of_keys:
            try:
                if 'Item' not in table.get_item(Key={"vs_md5": vs_key}):
                    fault_keys.append(vs_key)
            except Exception as e:
                fault_keys.append(vs_key)
                self.log.error(F"Error : {e}")
        return fault_keys

    def init_resources(self, resource_name, resource_region, table_name):
        list_of_keys_path = self.config["data"]["list_of_keys"]
        list_of_keys = load(open(list_of_keys_path + "/" + "list_of_keys.json", 'r'))

        dynamodb = resource(resource_name, resource_region)
        table = dynamodb.Table(table_name)

        return list_of_keys, table

    def main(self):
        resource_name = self.config["dynamo"]["resource_name"]
        resource_region = self.config["dynamo"]["resource_region"]
        table_name = self.config["dynamo"]["table_name"]
        fault_keys_path = self.config["data"]["fault_keys"]
        list_of_keys, table = self.init_resources(resource_name=resource_name,
                                                  resource_region=resource_region,
                                                  table_name=table_name)
        fault_keys = self.get_dynamo_keys(table=table, list_of_keys=list_of_keys)
        dump(fault_keys, open(fault_keys_path + "/" + "fault_keys.json", "w"))


if __name__ == '__main__':
    vm = ValidateMigration()
    vm.main()
