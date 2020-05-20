#!/usr/bin/python
# -*- coding: UTF-8 -*-

from elasticsearch import Elasticsearch, helpers


class ElasticsearchService:

    def __init__(self, hosts):
        self.__elasticsearch = Elasticsearch(hosts, sniff_on_start=False, sniffer_timeout=0,
                                             sniff_on_connection_fail=False, timeout=30, retry_on_timeout=True,
                                             max_retries=5)

    def search_scroll(self, index, query):
        try:
            return self.__elasticsearch.search(index=index, body=query,
                                               search_type="query_then_fetch", scroll="1m")
        except BaseException as e:
            print str(e)
            pass

        return {}

    def scroll_scan(self, query):
        try:
            resJson = self.__elasticsearch.scroll(body=query)
            return resJson
        except BaseException as e:
            print str(e)
            pass

        return []

    def insert_bulk(self, entries, es_index, es_doc_type, es=None):
        if es is None:
            es = self.__elasticsearch

        es_entries = []
        for doc in entries:
            entry = {"_index": es_index,
                     "_type": es_doc_type,
                     "_source": {k: v for k, v in doc['_source'].items() if k not in ['_id']}}

            if '_id' in doc.keys():
                entry['_id'] = doc['_id']

            es_entries.append(entry)

        helpers.bulk(es, es_entries, refresh=True, request_timeout=60)


if __name__ == '__main__':
    es_hosts_old = ["192.168.85.20:9200"]
    es_hosts_new = ['192.168.20.4:9210', '192.168.20.5:9210', '192.168.20.5:9211']
    # baike_all_indexs = ['shandong_bid_tender_v3', 'conversion_v1']
    baike_all_indexs = ['conversion_v1']
    baike_all_type = 'data'
    size = 1000
    elastic_service_old = ElasticsearchService(es_hosts_old)
    elastic_service_new = ElasticsearchService(es_hosts_new)

    for baike_all_index in baike_all_indexs:
        print baike_all_index + "数据同步开始！！！"
        # 这里是进行第一次查询，query中size指定每个批次的大小，返回的结果中不仅有查询到的数据，还有一个scroll_id， 这个scrool_id可以认为是下一次查询的起始位置
        res = elastic_service_old.search_scroll(baike_all_index, {"query": {"match_all": {}}, "size": size})
        hits = res.get('hits')
        all_total = hits.get('total')
        if all_total > 0:
            # 测试使用
            # for hit in hits.get('hits'):
            #     print hit['_id']
            #     print hit['_source']['title']
            #     print hit['_source']['detail_url']
            elastic_service_new.insert_bulk(hits.get('hits'), baike_all_index, baike_all_type)
            print "总数：" + str(all_total) + "条/第" + str(len(hits.get('hits'))) + "条"

        num = size
        while res.get('_scroll_id') and hits.get('total') > size:
            # 后续的每次查询都需要带上上一次查询结果中得到的scroll_id参数
            res = elastic_service_old.scroll_scan({'scroll': '1m', 'scroll_id': res.get('_scroll_id')})
            if 'hits' in res:
                hits = res.get('hits')
                if len(hits.get('hits')) > 0:
                    elastic_service_new.insert_bulk(hits.get('hits'), baike_all_index, baike_all_type)
                    num = len(hits.get('hits')) + num
                    print "总数：" + str(all_total) + "条/第" + str(num) + "条"
                else:
                    break
            else :
                print "res not contain hits"
        print baike_all_index + "数据同步结束！！！"
