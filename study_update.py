import time

from elasticsearch_obj import ElasticSearchObj


class Update(ElasticSearchObj):
    def refresh(self, index):
        self.client.indices.refresh(index=index)

    @staticmethod
    def cross_line():
        print('=================================================')

    def print_all_record(self, explain, all_record):
        for record in all_record.scan():
            print(explain, record.to_dict())
        self.cross_line()

    def update_example(self):
        # 将user_key id为90记录，name改为”我是来测试的“, height增加50
        # 查找user_key id为90更新前的记录
        update_before = self.search(self.user_key).filter("term", id=90)
        self.print_all_record("查找user_key id为90更新前的记录:", update_before)

        update_obj = self.update(self.user_key).filter(
            "term", id=90).script(
            source="ctx._source.name=params.name;ctx._source.height+=params.height;",
            params={
                "name": "我是来测试的",
                "height": 50
            })

        update_response = update_obj.execute()  # 请求执行并返回执行结果
        print("user_key id为90记录，name改为”我是来测试的“, height增加50 更新返回结果：", update_response)

        update_after = self.search(self.user_key).filter("term", id=90)
        self.print_all_record("查找user_key id为90更新后的记录1:", update_after)

        time.sleep(1)
        update_after = self.search(self.user_key).filter("term", id=90)
        self.print_all_record("查找user_key id为90更新后的记录2:", update_after)

        # 执行结果就不沾出来了，但有执行的可以看出
        # 更新后记录1：是更新前的结果； 而更新后记录2：是更新后的结果
        # 造成这一现象是因为上一个请求更新在内部并未完全完成，所以在sleep 一秒后便能获取更新后的记录
        print(end="\n\n\n")
        # 如果需要在更新后立即获取最新文档,又不知道多久能够完成更新, 可以在执行更新后刷新文档
        # 看下面示例
        # 将user_key 下id为70的文档，name改为”试试刷新index能否更新后获取最新数据“，height增加一百
        print("user_key 下id为70的文档，name改为”试试刷新index能否更新后获取最新数据“，height增加一百")
        update_before = self.search(self.user_key).filter("term", id=70)
        self.print_all_record("查找user_key id为80更新前的记录:", update_before)
        update_obj = self.update(self.user_key).filter(
            "term", id=70).script(
            source="ctx._source.name=params.name;ctx._source.height+=params.height;",
            params={
                "name": "试试刷新index能否更新后获取最新数据",
                "height": 100
            })

        update_response = update_obj.execute()  # 请求执行并返回执行结果
        print("user_key id为70记录，name改为”试试刷新index能否更新后获取最新数据“, height增加100 更新返回结果：", update_response)
        self.refresh(self.user_key)  # 刷新
        update_after = self.search(self.user_key).filter("term", id=70)
        self.print_all_record("查找user_key id为70更新后的记录1:", update_after)

        # 默认情况下，Elasticsearch 每秒都会定期刷新索引, 如果并不需要获取更新后的文档,尽量就不要手动刷新了
        # 可以通过更新响应的total跟updated数量是否一致判断记录是否更新成功
        # 查询更新会更新所有匹配的文档，查询条件跟上面介绍的查询用法一致
        # 例如:将user_key所有age增加一岁
        response = self.update(self.user_key).script(source="ctx._source.age+=1;").execute()
        print("将user_key所有age增加一岁:", response["total"], response["updated"], "response=", response)

        # 此处增加刷新，是因为上一个执行是更新整个user_key，如果还未自动刷新，执行下面示例，或造成并发异常，
        # 导致elasticsearch.exceptions.ConflictError异常
        self.refresh(self.user_key)
        print(end="\n\n\n")
        # 特别注意的是，如果script定义的字段，查询的文档存在则会更新，不存在则会在文档中插入字段
        print("将user_key下id为1的文档增加一个字段test_field，值为：[1,2,3]")
        update_before = self.search(self.user_key).filter("term", id=1)
        self.print_all_record("查找user_key id为1更新前的记录:", update_before)
        update_obj = self.update(self.user_key).filter(
            "term", id=1).script(
            source='ctx._source.test_field=params.test_field',
            params={
                "test_field": [1, 2, 3],
            })
        response = update_obj.execute()
        print("将user_key下id为1的文档增加一个字段test_field:", response)
        self.refresh(self.user_key)  # 刷新
        update_after = self.search(self.user_key).filter("term", id=1)
        self.print_all_record("查找user_key 将user_key下id为1的文档增加一个字段test_field，更新后的记录:", update_after)

        # 删除字段
        print("将user_key下id为1的文档增加的test_field字段移除")
        update_before = self.search(self.user_key).filter("term", id=1)
        self.print_all_record("查找user_key id为1更新前的记录:", update_before)
        response = self.update(self.user_key).filter(
            "term", id=1).script(
            source='ctx._source.remove("test_field")').execute()
        print("将user_key下id为1的文档增加的test_field字段移除:", response)
        self.refresh(self.user_key)  # 刷新
        update_after = self.search(self.user_key).filter("term", id=1)
        self.print_all_record("将user_key下id为1的文档增加的test_field字段移除，更新后的记录:", update_after)


if __name__ == "__main__":
    update = Update()
    update.update_example()

