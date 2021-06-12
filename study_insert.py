import random
from elasticsearch_obj import ElasticSearchObj


class CreateNewRecord(ElasticSearchObj):
    def index(self, index, doc_id=None, doc_type="_doc", **kwargs):
        """
        如果索引不存在，则会创建索引并写入文档
        self.client.index(index, body, doc_type=None, id=None, params=None, headers=None)
        http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
        :param index: 文档索引名
        :param doc_id: 当doc_id=None时，通过该方法会向文档中插入一条记录，doc_id!=None时，如果doc_id已经存在则会更新，不存在则插入
        :param doc_type: 文档类型，默认为_doc，相同index下的记录值必须一致，如果不相等，则会触发异常
        :param kwargs: 文档内容
        :return:
        """
        return self.client.index(index=index, body=kwargs, doc_type=doc_type, id=doc_id)

    def create(self, index, doc_id=None, doc_type="_doc", **kwargs):
        """
        http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
        用法同index，不同的是create必须传递文档Id，否则会触发异常错误
        :param index:
        :param doc_id:
        :param doc_type:
        :param kwargs:
        :return:
        """
        return self.client.create(index=index, id=doc_id, doc_type=doc_type, body=kwargs)

    def test_insert(self):
        """
        插入一些记录，方便等会查询时使用
        :return:
        """
        hobby = ["唱", "跳", "rap", "唱跳rap", "游泳", "打球", "其他"]
        for i in range(100):
            response = self.index(
                index=self.user_key,
                id=i,
                name="".join(random.sample('zyxwvutsrqponmlkjihgfedcba', random.randint(3, 8))),
                age=random.randint(18, 30),
                hobby=random.choice(hobby),
                height=random.randint(155, 185),
                sex=random.choice(["男", "女", "不详"])
            )
            print(f"{self.user_key}插入{i}条数据， 返回：{response}")

        for i in range(100):
            response = self.index(
                index=self.friend_key,
                user_id=i,
                friend_id=random.randint(0, 99),
                word=random.sample(["拧螺丝", "撸代码", "CRUD", "划水", "偷偷写下bug"], random.randint(1, 3)),
                other={"work_place": random.choice(["第一排", "第二排"]), "performance": random.choice(["好", "不好"])}
            )
            print(f"{self.friend_key}插入{i}条数据， 返回：{response}")

        for i in range(20000):
            response = self.index(
                index=self.test_count_key,
                user_id=i,
                friend_id=random.randint(0, 99),
                weight=random.randint(0, 10000)
            )
            print(f"{self.test_count_key}插入{i}条数据， 返回：{response}")


if __name__ == "__main__":
    test = CreateNewRecord()
    test.test_insert()
