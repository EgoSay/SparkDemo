# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/1/22 上午9:55
# @IDE     : PyCharm


from Init_SparkContext import MySparkContext

sc = MySparkContext().open()

lines = sc.parallelize(["sacajawea", "recedes", "hello", "haft vaderA Sabbath", "www"])

# 使用第一个单词作为键(这里是以空格划分单词)创建出一个 pair RDD
pairs = lines.map(lambda x: (x.split(" ")[0], x))
# 根据字符长度进行筛选
result = pairs.filter(lambda KeyValue: len(KeyValue[1]) > 7)
print(result.collect())


"""
map和flatMap的区别:
map():对每一条输入进行指定的操作，然后为每一条输入返回一个对象
flatMap(): 操作1：同map函数一样：对每一条输入进行指定的操作，然后为每一条输入返回一个对象
           操作2：最后将所有对象合并为一个对象
"""
rdd = sc.parallelize([2, 3, 4])
print(rdd.map(lambda x: list(range(1, x))).collect())
print(rdd.flatMap(lambda x: range(1, x)).collect())

