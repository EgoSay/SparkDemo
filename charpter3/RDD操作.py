# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/1/17 下午5:46
# @IDE     : PyCharm



from Init_SparkContext import MySparkContext

# 读取数据（因为spark的惰性求值，此时并没有真正读取进来数据）
log_path = '../../test.log'
rdd = MySparkContext().read_data(log_path)

# 用filter进行转化操作
errorsRDD = rdd.filter(lambda s: "error" in s and "INFO" not in s)
warningsRDD = rdd.filter(lambda s: "WARN" in s)

# 进行union转化操作
badLinesRDD = errorsRDD.union(warningsRDD)

# 行动操作对错误进行计数
print("Input concerning lines:%d"% badLinesRDD.count())
print("\nHere are 10 examples:")
for line in badLinesRDD.take(10):
    print(line)

"""
RDD 还有一个 collect() 函数，可以用来获取整个RDD 中的数据
但是只有当你的整个数据集能在单台机器的内存中放得下时，才能使用collect()

每当我们调用一个新的行动操作时，整个RDD都会从头开始计算。要避免这种低效的行为，用户可以将中间结果持久化
"""
