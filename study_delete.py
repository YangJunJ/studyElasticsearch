from elasticsearch_obj import ElasticSearchObj


class StudyDelete(ElasticSearchObj):
    def delete_index(self, index):
        """
        https://www.elastic.co/guide/en/elasticsearch/reference/7.9/indices-delete-index.html
        删除整个索引
        :param index:
        :return:
        """
        try:
            self.client.indices.delete(index=index)
        except Exception:
            print(f"需要删除的索引：{index}不存在")

    def delete_by_query(self):
        """
        示例根据查询条件删除文档
        :return:
        """

        # 将user_key下id大于90的玩家删除
        all_record = self.search(self.user_key)
        print("查看删除前文档的数量：", all_record.count())
        all_record.filter("range", id={"gt": 90}).delete()
        self.client.indices.refresh(index=self.user_key)
        all_record = self.search(self.user_key)
        print(f"查看删除后文档的数量：{all_record.count()}")


if __name__ == "__main__":
    study_delete = StudyDelete()
    study_delete.delete_index(study_delete.friend_key)  # 删除索引friend_key
    study_delete.delete_by_query()

