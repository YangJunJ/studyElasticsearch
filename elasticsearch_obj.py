from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, UpdateByQuery


class ElasticSearchObj(object):
    def __init__(self):
        self.user_key = "user_index"
        self.friend_key = "friend_index"
        self.test_count_key = "count_test_key"
        # 初始化一个Elasticsearch实例对象
        self.client = Elasticsearch(hosts=[{'host': "127.0.0.1", "port": 9200}])

    def search(self, index):
        # 返回索引对象结果级
        return Search(using=self.client, index=index)

    def update(self, index):
        # ubq = UpdateByQuery(index=index).using(self.client)
        # or
        ubq = UpdateByQuery(index=index, using=self.client)
        return ubq





