# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/1/19 下午6:07
# @IDE     : PyCharm

"""
两个最常用的转化操作是 map() 和 filter()
    - map: 接收一个函数，把这个函数用于 RDD 中的每个元素，将函数的返回结果作为结果RDD中对应元素的值
    - filter: 接收一个函数，并将 RDD 中满足该函数的 元素放入新的 RDD 中返回
"""
from Init_SparkContext import MySparkContext


sc = MySparkContext().open()

# 利用map计算平方
nums = sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: x * x).collect()
for num in squared:
    print("%i" % num)

"""
提供给 flatMap() 的函数被分别应用到了输入 RDD 的每个元素上。不 过返回的不是一个元素，而是一个返回值序列的迭代器。
输出的 RDD 倒不是由迭代器组 成的。我们得到的是一个包含各个迭代器可访问的所有元素的 RDD
"""
# 利用flatMap()将行数据切分为单词
lines = sc.parallelize(["hello world", "hi"])
words = lines.flatMap(lambda line: line.split(""))
print(words.first())



