# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/2/13 下午7:25
# @IDE     : PyCharm

from Init_SparkContext import MySparkContext

sc = MySparkContext().open()

"""累加空行"""
file = sc.textFile('../test.log')
# 创建Accumulator[int]并初始化为0
blankLines = sc.accumulator(0)


def extractCallSigns(line):
    global blankLines  # 访问全局变量
    if (line == ""):
        blankLines += 1
    return line.split(" ")


callSigns = file.flatMap(extractCallSigns)
callSigns.saveAsTextFile("callSigns")
print("Blank lines: %d" % blankLines.value)
