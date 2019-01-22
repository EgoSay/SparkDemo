# @Author  : adair_chan
# @Email   : adairchan.dream@gmail.com
# @Date    : 2019/1/19 下午4:24
# @IDE     : PyCharm


from Init_SparkContext import MySparkContext

# 向spark传递函数
log_path = '../../test.log'
rdd = MySparkContext().read_data(log_path)


def contains_error(s):
    return "error" in s


word = rdd.filter(contains_error)


# 传递一个带字段引用的函数
class SearchFunctions(object):
    def __init__(self, query):
        self.query = query

    def is_match(self, s):
        return self.query in s

    # 以下两种方法不推荐
    def get_matches_function_reference(self, rdd):
        # 问题:在"self.isMatch"中引用了整个self
        return rdd.filter(self.is_match)

    def get_matches_member_reference(self, rdd):
        # 问题:在"self.query"中引用了整个self
        return rdd.filter(lambda x: self.query in x)

    # 替代上面写法的方案，把所需要的字段从对象中拿出来放到一个局部变量中，然后传递这个局部变量
    def get_matches_no_reference(self, rdd):
        query = self.query
        return rdd.filter(lambda x: query in x)

