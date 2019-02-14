# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/2/13 下午7:25
# @IDE     : PyCharm

"""
注意，工作节点上的任务不能访问累加器的值。从这些任务的角度来看，累加器是一个只写变量
"""
import re
from _operator import add

from Init_SparkContext import MySparkContext

sc = MySparkContext().open()

# 累加空行
file = sc.textFile('../test.log')
# 创建Accumulator[int]并初始化为0
blankLines = sc.accumulator(0)


def extractCallSigns(line):
    global blankLines  # 访问全局变量
    if line == "":
        blankLines += 1
    return line.split(" ")


callSigns = file.flatMap(extractCallSigns)
callSigns.saveAsTextFile("callSigns")
print("Blank lines: %d" % blankLines.value)

# 使用累加器进行呼号计数
validSignCount = sc.accumulator(0)
invalidSignCount = sc.accumulator(0)


def validateSign(sign):
    global validSignCount, invalidSignCount
    if re.match(r"\A\d?[a-zA-Z]{1, 2}\d{1, 4}[a-zA-Z]{1, 3}\Z", sign):
        validSignCount += 1
        return True
    else:
        invalidSignCount += 1
        return False


# 对与每个呼号的联系次数进行计数
validSigns = callSigns.filter(validateSign)
contactCount = validSigns.map(lambda sign: (sign, 1)).reduceByKey(add)

# 强制求值计算计数
print(contactCount.count(), validSignCount, invalidSignCount)
if invalidSignCount.value < 0.1 * validSignCount.value:
    contactCount.saveAsTextFile("contactCount")
else:
    print("Too many errors")