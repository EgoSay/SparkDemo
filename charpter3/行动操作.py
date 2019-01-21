# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/1/19 下午7:00
# @IDE     : PyCharm

"""
reduce(): 接收一个函数作为参数，这个 函数要操作两个 RDD 的元素类型的数据并返回一个同样类型的新元素
fold(): fold() 和 reduce() 类似，接收一个与 reduce() 接收的函数签名相同的函数，再加上一个 “初始值”来作为每个分区第一次调用时的结果。
        例如，加法初始值应为0，乘法初始值应为1
aggregate(): 可以对两个不同类型的元素进行聚合，即支持异构
"""
from _operator import add

from Init_SparkContext import MySparkContext

sc = MySparkContext().open()
rdd = sc.parallelize([121, 28, 63, 214, 88])
# 记录遍历过程中的计数以及元素的数量

# 第一种方法
total = rdd.map(lambda x: 1).reduce(add)
# total3 = sc.reduce(lambda x, y: x + y)
# print(total3)
average = rdd.map(lambda x: x).reduce(add) / total
print("总数:{0},平均值为:{1}".format(total, average))
print("总数:%f,平均值为:%f" % (total, average))

# 第二种方法
nums = rdd.map(lambda x: (x, 1))
total2 = nums.values().sum()
average2 = nums.keys().sum()

"""
第三种方法，利用aggregate函数
>>> rdd = sc.parallelize([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
>>> seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
>>> combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
seqOp过程：把数据分区计算，一个rdd二元组 + 一个数
        partition One
            (0,0) 9
            (9,1) 4
            (13,2) 1
            (14,3)
        partition Two
            (0,0) 6
            (6,1) 2
            (8,2) 0
            (8,3)
        partition Three
            (0,0) 7
            (7,1) 5
            (12,2)
        partition Four
            (0,0) 3
            (3,1) 8
            (11,2)
combOp过程：聚合计算每个分区的结果然后统计计算得出最终结果，不同分区的rdd 二元组相加
        (14,3) (12,2) ===>> (26, 5)
        (8,3) (11,2) ===>> (19, 5)
        (26, 5) (19, 5) ===>> (45, 10) 最终结果
"""
seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
sumCount = rdd.aggregate((0, 0), seqOp, combOp)
print("总数:%f,平均值为:%f" % (sumCount[1], sumCount[0] / sumCount[1]))



