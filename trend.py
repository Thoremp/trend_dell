#coding=utf-8

import urllib
from lxml import etree
import htmllist
import trendUtil
import re
import time
import sys
import logging
from multiprocessing.dummy import Pool
reload(sys)
sys.setdefaultencoding('utf-8')
"""
    信息顺序:标题,公司名字,公司网址,月薪,工作地点,发布日期,工作性质,工作经验,最低学历,招聘人数,职位类别
    最重要的是人数和职位类别
"""

# 循环搜索所有类别 返回数组urls-102个url
def getAllUrls():

    # logging.info("开始!")
    # 声明一个数组存放所有的 url 集合
    urls = []

    # 变换 bj属性和 sj属性 获得所有职位分类的 url
    # 1.从数据库获得所有的 bj和 sj(相对应的)
    result = trendUtil.getClassAndCode()
    # 2.循环赋值获得 url
    for each in result:
        sj = each[0]
        bj = each[1]
        params = urllib.urlencode({'bj':bj,'sj':sj,'p':1,'isadv':0,'jl':'全国'})
        url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?%s' % params
        urls.append(url)
    return urls

# 获取所有详细信息的url 输出urlIns
def printUrl(url):

    print url + " - " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    try:
        html = urllib.urlopen(url, data=None, timeout=3).read()
    except:
        print "解析url出现错误:" + url
    else:
        selector = etree.HTML(html)
        # 获取内部详细信息的网址
        secondurl = selector.xpath('//td[@class="zwmc"]/div/a/@href')

        for each in secondurl:
            if each == 'http://xiaoyuan.zhaopin.com/zhuanti/first.html':
                continue
            if each == 'http://e.zhaopin.com/products/1/detail.do':
                continue
            if each == 'http://xiaoyuan.zhaopin.com/first/':
                continue
            if each[0:27] == 'http://xiaoyuan.zhaopin.com':
                continue

            print each + " - " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            #保存数据库之前查一下在trend_basis中是否已经存在该url,如果存在就不保存了
            count = trendUtil.judgeUrlBasis(each)
            if(count == 0):
                #保存到数据库
                trendUtil.saveAllUrl(each)


    # 获取下一页按钮的网址(如果有的话)
    try:
        content = selector.xpath('//li[@class="pagesDown-pos"]/a/@href')
    except:
        print "解析下一页url出现错误:" + url
        # logging.info("解析下一页url出现错误:" + url)
    else:
        if content:
            content = content[0]
            printUrl(content)
        else:
            print "end"

# 处理详细信息页,存储数据库
def saveUrl(each):

    each = each[1]
    print "正在保存:" + each + " - " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    # logging.info("正在保存:" + each + " - " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    # 存储数据库之前先检查数据库有没有这条信息,如果有则不存了
    result = trendUtil.isBasisByUrl(each)
    if result == 'have':
        print "数据库已存在此数据:" + each
        # logging.info("数据库已存在此数据:" + each)
    else:

        try:
            item = innerHtml(each)
        except:
            print "解析url失败:" + each
            # logging.info("解析url失败:" + each)
        else:

            try:
                trendUtil.saveTrendBasis(item)  # 存储数据库
            except:
                print "保存失败:" + each
                # logging.info("保存失败:" + each)
            else:
                print "保存成功:" + each
                # logging.info("保存成功:" + each)

    # 在 trend_url 中将此 url 的 status 置为 1
    trendUtil.setStatusByUrl(each)

# 获取详细信息(从内部详细信息网页) 返回数组item
def innerHtml(url):
    "需要输入进入详细页面的网址,对内部页面读取详细信息"
    item = []
    item.append(url)
    # htmlSecond = htmllist.html1 # 测试用
    htmlSecond = urllib.urlopen(url).read() #真实代码

    # 标题
    selectorSecond = etree.HTML(htmlSecond)
    position = selectorSecond.xpath('//h1/text()')
    if position:
        position = position[0]
        position = position.replace('🔴', '')
    item.append(position)

    # 公司名称 和 公司主页url
    company = selectorSecond.xpath('//h2/a/text()')
    companyUrl = selectorSecond.xpath('//h2/a/@href')
    if company:
        company = company[0]
    if companyUrl:
        companyUrl = companyUrl[0]
    item.append(company)
    item.append(companyUrl)

    selectorThird = selectorSecond.xpath('//div[@class="terminalpage clearfix"]/div[@class="terminalpage-left"]/ul/li')
    time.sleep(0.5)
    # 工资
    name = selectorThird[0].xpath('span/text()')
    value = selectorThird[0].xpath('strong/text()')
    if name:
        name = name[0]
    if value:
        value = value[0]
    item.append(value)

    # 工作地址
    name = selectorThird[1].xpath('span/text()')[0]
    value = selectorThird[1].xpath('strong/a/text()')[0]
    item.append(value)

    # 发布日期
    name = selectorThird[2].xpath('span/text()')[0]
    value = selectorThird[2].xpath('strong/span/text()')[0]
    # value = re.findall('<strong><span id="span4freshdate">(.*?)</span></strong>',htmlSecond,re.S)
    item.append(value)

    # 工作性质
    name = selectorThird[3].xpath('span/text()')[0]
    value = selectorThird[3].xpath('strong/text()')[0]
    item.append(value)

    #工作经验
    name = selectorThird[4].xpath('span/text()')[0]
    value = selectorThird[4].xpath('strong/text()')[0]
    item.append(value)

    # 最低学历
    name = selectorThird[5].xpath('span/text()')[0]
    value = selectorThird[5].xpath('strong/text()')[0]
    item.append(value)

    # 招聘人数 30人
    name = selectorThird[6].xpath('span/text()')[0]
    value = selectorThird[6].xpath('strong/text()')[0]
    value1 = re.findall('(\d+)',value,re.S)
    item.append(value1[0])

    # 职位类别
    name = selectorThird[7].xpath('span/text()')[0]
    value = selectorThird[7].xpath('strong/a/text()')[0]
    item.append(value)

    # 公司一些信息
    selectorThird = selectorSecond.xpath('//div[@class="company-box"]/ul/li')
    # 公司规模：100-499人公司
    name = selectorThird[0].xpath('span/text()')[0]
    value = selectorThird[0].xpath('strong/text()')[0]
    item.append(value)

    # 性质：股份制企业公司
    name = selectorThird[1].xpath('span/text()')[0]
    value = selectorThird[1].xpath('strong/text()')[0]
    item.append(value)

    # 行业：计算机软件
    name = selectorThird[2].xpath('span/text()')[0]
    value = selectorThird[2].xpath('strong/a/text()')[0]
    item.append(value)

    # 公司地址： 北京市海淀区丰惠中路7号新材料创业大厦
    name = selectorThird[3].xpath('span/text()')[0]
    name = name.replace(':',"")[0:4]
    if name == '公司地址':
        value = selectorThird[3].xpath('strong/text()')[0]
        value = value.strip()
        item.append(value)
    else:
        value = selectorThird[4].xpath('strong/text()')[0]
        value = value.strip()
        item.append(value)
    return item

# 保存所有详细信息url
def saveInurl(each):
    urls= []
    print "正在保存:" + each
    html = urllib.urlopen(each).read()
    selector = etree.HTML(html)
    secondurl = selector.xpath('//td[@class="zwmc"]/div/a/@href')
    for e in secondurl:
        urls.append(e)
    return urls

if __name__ == '__main__':

    # while 1:
    #     try:
    #         # 2.解析url(使用多线程)
    #         trendUtil.deleteTrendUrl() # 删除不合格的url
    #         result = trendUtil.getUrlBystatus()
    #         pool = Pool()  # 初始化一个实例,4代表四线程,不写参数将使用机器核心数作为参数
    #         pool.map(saveUrl, result)  # 实现爬取
    #         pool.close()
    #         pool.join()  # join()作用:等待爬取完成以后再回到下面一行代码的执行
    #     except:
    #         print u'多线程出现错误 - ' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    #         continue
    #
    #     try:
    #         # 1.取出url 并保存到数据库 每天一次
    #         urls = getAllUrls()
    #         pool = Pool()  # 初始化一个实例,4代表四线程,不写参数将使用机器核心数作为参数
    #         pool.map(printUrl, urls)
    #         pool.close()
    #         pool.join()  # join()作用:等待爬取完成以后再回到下面一行代码的执行
    #     except:
    #         print u'多线程出现错误 - ' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    #         continue

    trendUtil.deleteTrendUrl()  # 删除不合格的url
    result = trendUtil.getUrlBystatus_dell()
    pool = Pool()
    pool.map(saveUrl, result)
    pool.close()
    pool.join()