# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/1/25 下午2:28
# @IDE     : PyCharm
import json

from Init_SparkContext import MySparkContext

sc = MySparkContext().open()

# 读取文本文件
text = sc.textFile('../test.log')
info = text.filter(lambda s: "error" in s)
info.saveAsTextFile('test')

# 读取json数据
data = sc.textFile('test.json')
data = data.map(lambda x: json.loads(x))
data.foreach(lambda x: print(x))