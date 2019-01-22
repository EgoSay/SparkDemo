# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/1/22 下午4:55
# @IDE     : PyCharm

from _operator import add
from Init_SparkContext import MySparkContext

sc = MySparkContext().open()

fruits = sc.parallelize([("a", ["apple", "banana", "lemon"]), ("b", ["grapes"]), ("a", ["apple2", "banana2"])])

"""
mapValues():对键值对RDD中的每个value都应用一个函数，但是，key不会发生变化
reduceByKey():对具有相同key的键值对进行合并
"""
t = fruits.mapValues(lambda x: len(x))
t2 = t.reduceByKey(add)
print("\n{0}\n{1}".format(t.collect(), t2.collect()))
# print("\n{0}\n{1}".format(t.collect(), type(t2)))
t3 = fruits.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print(t3.collect())


"""
combineByKey() 是最为常用的基于键进行聚合的函数。大多数基于键聚合的函数都是用它 实现的
如果这是一个新的元素，combineByKey()会使用一个叫作 createCombiner() 的函数来创建那个键对应的累加器的初始值。
    -- 需要注意的是，这一过程会在每个分区中第一次出现各个键时发生，而不是在整个 RDD 中第一次出现一个键时发生。
如果这是一个在处理当前分区之前已经遇到的键，它会使用 mergeValue() 方法将该键的累 加器对应的当前值与这个新的值进行合并。
由于每个分区都是独立处理的，因此对于同一个键可以有多个累加器。
如果有两个或者更 多的分区都有对应同一个键的累加器，就需要使用用户提供的 mergeCombiners() 方法将各 个分区的结果进行合并。
"""
people = [("男", "李四"), ("男", "张三"), ("女", "韩梅梅"), ("女", "李思思"), ("男", "马云")]
rdd = sc.parallelize(people, 2)
result = rdd.combineByKey((lambda x: (x, 1)),
                          (lambda x, y: (x[0] + "," + y, x[1] + 1)),
                          (lambda x, y: (x[0] + "," + y[0], x[1] + y[1])))
result.foreach(print)
