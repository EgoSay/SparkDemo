# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/1/23 下午6:42
# @IDE     : PyCharm

# !/usr/bin/env python
# -*- coding: utf-8 -*-

""" PageRank算法
运行: bin/spark-submit files/pagerank.py data/mllib/pagerank_data.txt 10
"""
from __future__ import print_function

import re
import sys
from operator import add

from pyspark import SparkConf, SparkContext


def compute_contribs(urls, rank):
    """ 给urls计算
    Args:
        urls: 目标url相邻的urls集合
        rank: 目标url的当前rank

    Returns:
        url: 相邻urls中的一个url
        rank: 当前url的新的rank
    """
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def split_url(url_line):
    """ 把一行url切分开来
    Args:
        url_line: 一行url，如 1 2
    Returns:
        url, neighbor_url
    """
    parts = re.split(r'\s+', url_line)  # 正则
    return parts[0], parts[1]


def compute_pagerank(sc, url_data_file, iterations):
    """ 计算各个page的排名
    Args:
        sc: SparkContext
        url_data_file: 测试数据文件
        iterations: 迭代次数
    Returns:
        status: 成功就返回0
    """

    # 读取url文件 ['1 2', '1 3', '2 1', '3 1']
    lines = sc.textFile(url_data_file)

    # 建立Pair RDD (url, neighbor_urls) [(1,[2,3]), (2,[1]), (3, [1])]
    links = lines.map(lambda line: split_url(line)).distinct().groupByKey().mapValues(lambda x: list(x)).cache()
    # 初始化所有url的rank为1 [(1, 1), (2, 1), (3, 1)]
    ranks = lines.map(lambda line: (line[0], 1))

    for i in range(iterations):
        # (url, [(neighbor_urls)]) join neighbor_urls and rank ==> (url, [(neighbor_urls), rank])
        # 把当前url的rank分别contribute到其他相邻的url (url, rank)
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: compute_contribs(url_urls_rank[1][0], url_urls_rank[1][1])
        )
        # 把url的所有rank加起来，再赋值新的
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    for (link, rank) in ranks.sortBy((lambda x: x[1]), ascending=False).collect():
        print("%s has rank %s." % (link, rank))

    return 0


if __name__ == '__main__':

    # 数据文件和迭代次数
    url_data_file = '../pagerank_data.txt'
    iterations = int(8)

    # 配置 SparkContext
    conf = SparkConf().setAppName('PythonPageRank')
    conf.setMaster('local')
    sc = SparkContext(conf=conf)

    ret = compute_pagerank(sc, url_data_file, iterations)
    sys.exit(ret)
