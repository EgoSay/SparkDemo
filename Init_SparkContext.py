# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/1/18 上午10:02
# @IDE     : PyCharm
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class MySparkContext():

    def __init__(self):
        self.conf = SparkConf().setMaster("local").setAppName("My App")
        self.sc = SparkContext(conf=self.conf)

    def open(self):
        return self.sc

    def close(self):
        self.sc.stop()

    def getSparkSession(self):
        return SparkSession(self.sc)

    def read_data(self, path):
        return self.sc.textFile(path)
