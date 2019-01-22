# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/1/19 下午6:38
# @IDE     : PyCharm

"""
常见集合操作：
    -union(): 返回一个包含两个 RDD 中所有元素的 RDD
    -intersection(): 只返回两个 RDD 中都有的元素, 会去掉所有重复的元素(单个 RDD 内的重复元素也会一起移除), 性能较差
    -distinct(): 生成一个只包含不同元素的新 RDD
    -subtract(): 收另一个 RDD 作为参数，返回 一个由只存在于第一个 RDD 中而不存在于第二个 RDD 中的所有元素组成的 RDD, 性能较差
    -cartesian(): 计算两个 RDD 的笛卡儿积
其中distinct()，intersection()，subtract()性能较差是因为它们都需要将所有数据通过网络进行混洗（数据混洗）
"""
