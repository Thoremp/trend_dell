# -*- coding:utf8 -*-

import trend
import urllib
from lxml import etree
import time
from multiprocessing.dummy import Pool
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# urls = trend.getAllUrls()
# url = urls[0]

def printUrl(url):

    print "正在保存:" + url

    html = urllib.urlopen(url).read()
    selector = etree.HTML(html)

    # 获取内部详细信息的网址
    secondurl = selector.xpath('//td[@class="zwmc"]/div/a/@href')

    for each in secondurl:
        if each == 'http://xiaoyuan.zhaopin.com/zhuanti/first.html':
            continue
        if each == 'http://e.zhaopin.com/products/1/detail.do':
            continue

        try:
            item = trend.innerHtml(each)
        except:
            continue
        else:
            print "正在保存:" + item[0]

    # 获取下一页按钮的网址(如果有的话)
    content = selector.xpath('//li[@class="pagesDown-pos"]/a/@href')
    if content:
        content = content[0]
        # printUrl(content)
    else:
        print "end"

def printUrl2(url):

    print "正在保存:" + url

    html = urllib.urlopen(url).read()
    selector = etree.HTML(html)

    # 获取内部详细信息的网址
    secondurl = selector.xpath('//td[@class="zwmc"]/div/a/@href')

    pool = Pool(4)  # 初始化一个实例,4代表四线程
    time3 = time.time()
    pool.map(trend.innerHtml(url), secondurl)  # 实现爬取
    pool.close()
    pool.join()  # join()作用:等待爬取完成以后再回到下面一行代码的执行
    time4 = time.time()
    print u'并行用时:' + str(time4 - time3)


    # 获取下一页按钮的网址(如果有的话)
    content = selector.xpath('//li[@class="pagesDown-pos"]/a/@href')
    if content:
        content = content[0]
        # printUrl(content)
    else:
        print "end"

if __name__ == "__main__":
    url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?bj=160000&sj=079&sm=0&p=1'

    # time1 = time.time()
    # printUrl(url)
    # time2 = time.time()
    # print u'单线程用时:' + str(time2 - time1)

    # time1 = time.time()
    # printUrl2(url)
    # time2 = time.time()
    # print u'多线程用时:' + str(time2 - time1)


