from elasticsearch_obj import ElasticSearchObj


class Search(ElasticSearchObj):
    """
        示例 elasticsearch_dsl.Search 基本操作

        查询的字段值中如果包含中文、下划线等其他特殊字符， 字段需要需要加上 __keyword,否则无法匹配， 用法详见示例


        介绍两个查询中需要使用到的方法：
            elasticsearch_dsl.Search.scan()： 将搜索条件扫描索引，返回所有与条件匹配文档生成器
            elasticsearch_dsl.Search.to_dict(): 将hit对象系列化成python字典记录
    """
    def get_all_record(self):
        """
        示例获取self.key下的所有文档
        :return:
        """
        # all_record 是 elasticsearch_dsl.search.Search对象，可以通过scan() 方法遍历self.key索引内的所有文档
        all_record = self.search(self.user_key)
        for record in all_record.scan():
            # record 是 elasticsearch_dsl.response.hit.Hit 对象
            # 可以通过.属性获取字段值， 比如: record.name
            # 如果字段名不存在，则会触发'Hit' object has no attribute 错误
            print(f"record.id:{record.id}, record.name:{record.name}")

            # 上面的是其中一种方法，它也可以通过to_dict()方法，将记录通过python字典的形式返回
            print(record.to_dict())

    def print_all_record(self, explain, all_record, max_count=3):
        i = 1
        for record in all_record.scan():
            print(explain, record.to_dict())
            if i >= max_count:
                print(explain, f"查询数量大于{i}，后面的记录不打印了")
                break
            i += 1
        self.cross_line()

    @staticmethod
    def cross_line():
        print('=================================================')

    def get_record_by_query(self):
        """
        示例query的常用用法
        :return:
        """
        all_record = self.search(self.user_key)

        # 例如要找到爱好中包含rap的用户信息
        match_rap = all_record.query("match", hobby="rap")
        self.print_all_record("查看所有包含爱好rap的用户:", match_rap)

        # 查找爱好中含有rap，并且性别为女的用户信息
        match_rap_female = all_record.query("match", hobby="rap").query("term", sex__keyword="女")
        # match_rap_female = match_rap.query("term", sex__keyword="女")
        self.print_all_record("查看爱好含有rap的所有女用户：", match_rap_female)

        # 查找身高大于175的男用户，并且爱好只有唱的用户
        man_sing_gt_175 = all_record.query("range", height={"gt": 175}).query(
            "term", sex__keyword="男").query(
            "term", hobby__keyword="唱")
        self.print_all_record("查找身高大于175的男用户，并且爱好只有唱的用户", man_sing_gt_175)

    def get_record_by_filter(self):
        """
        示例filter的用法
        filter 与 query用法一样，这里只举一个简单例子
        :return:
        """
        all_record = self.search(self.user_key)

        # 查找年龄大于等于25的男用户，并且爱好是唱跳rap
        all_record = all_record.filter("range", age={"gte": 25}).filter(
            "term", sex__keyword="男").filter(
            "term", hobby__keyword="唱跳rap")
        self.print_all_record("查找年龄大于等于25的男用户，并且爱好是唱跳rap:", all_record)

    def get_record_by_query_and_filter(self):
        """
        query、filter可以混用
        举一个例子
        :return:
        """
        all_record = self.search(self.user_key)

        # 查找年龄在20-25的男性用户
        all_record = all_record.query("range", age={"gte": 20, "lte": 25}).filter("term", sex__keyword="男")
        self.print_all_record("查找年龄在20-25的男性用户:", all_record)

    def get_record_by_exclude(self):
        """
        非查询
        :return:
        """
        all_record = self.search(self.user_key)

        # 查找性别不是男生，并且爱好不是唱的用户
        records = all_record.exclude("term", sex__keyword="男").exclude("term", hobby__keyword="唱")
        self.print_all_record("查找性别不是男生，并且爱好不是唱的用户:", records, 6)

        # 查找男性用户中，爱好不是唱且身高大于175的用户
        records = all_record.filter("term", sex__keyword="男").exclude(
            "term", hobby__keyword="唱").query(
            "range", height={"gt": 175})
        self.print_all_record("查找男性用户中，爱好不是唱且身高大于175的用户:", records, 6)

    def other_search(self):
        """
        其他查询：
            获取文档数量:.count()
            查询结果排序：sort(field)
        :return:
        """
        all_record = self.search(self.user_key)

        # 获取id>=90的文档数量
        gte_90 = all_record.filter("range", id={"gte": 90})
        print(f"id>=90的文档 存在，数量为：{gte_90.count()}")  # id>=90的文档 存在，数量为：10

        # 获取id>=300的文档数量
        gte_300 = all_record.filter("range", id={"gte": 300})
        print(f"id>=300的文档 存在，数量为：{gte_300.count()}")  # id>=300的文档 存在，数量为：0

        #  排序 获取排序结果后，不能使用san(), 比如：self.search(self.key).sort("-height").scan()
        #  使用scan()不会以任何预先确定的顺序返回结果, 详见：
            # https://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.scan
        # 不使用san() 获取记录会有数量10000条的限制，如果查询文档数量大于10000，不使用san（）方法便会出现异常
        # 下面查询会触发异常：
        #     all_record = self.search(self.test_count_key)
        #     for record in all_record[0: 15000]:
        #         print(record.to_dict())
        #  因为test_count_key下数量已经达到20000条，当取15000条数据时会触发：elasticsearch.exceptions.RequestError异常
        #  查询id>90, 且按玩家身高排序从大到小排序
        gte_90_sort = all_record.sort("-height")
        # self.print_all_record("查询id>90, 且按玩家身高排序从大到小排序:", gte_90_sort, 10)
        for record in gte_90_sort:
            print(record.to_dict())
        self.cross_line()
        #  查询id>90, 且按玩家身高排序从小到大排序, 如果身高相同，则按id降序排序
        gte_90_sort = all_record.filter("range", id={"gte": 90}).sort("height", "-id")
        for record in gte_90_sort:
            print(record.to_dict())
        self.cross_line()

        # 分页
        # 查询用户用户数据，跳过前五十条后20条
        skip_50_search_20 = all_record[50: 70]
        i = 1
        for record in skip_50_search_20:
            print(f"第{i}条数据， 用户id：{record.to_dict()['id']}")
            i += 1


if __name__ == "__main__":
    search = Search()
    # search.get_all_record()
    # search.get_record_by_query()
    # search.get_record_by_filter()
    # search.get_record_by_query_and_filter()
    # search.get_record_by_exclude()
    search.other_search()

