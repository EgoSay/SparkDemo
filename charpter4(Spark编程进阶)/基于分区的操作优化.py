# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/2/19 上午11:09
# @IDE     : PyCharm

"""
利用分区优化charpter1行动操作.py的平均值计算过程
每个分区只创建一次二元组，而不用为每个元素都执行这个操作
"""

from _operator import add

from Init_SparkContext import MySparkContext

sc = MySparkContext().open()
rdd = sc.parallelize([121, 28, 63, 214, 88])


def partitionCtr(nums):
    """计算分区的sumCounter"""
    sumCount = [0, 0]
    for num in nums:
        sumCount[0] += num
        sumCount[1] += 1
    return [sumCount]


def fastAvg(nums):
    """计算平均值"""
    sumCount = nums.mapPartitions(partitionCtr).reduce(add)
    print("计算所得总和为:%f,平均值为:%f" % (sumCount[0], sumCount[0] / float(sumCount[1])))


if __name__ == '__main__':
    fastAvg(rdd)
