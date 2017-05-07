# -*- coding:utf8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import htmllist
import urllib
from lxml import etree
import MySQLdb
import re
import time
from DBUtils.PooledDB import PooledDB # 创建数据库连接池
pool = PooledDB(MySQLdb, 10, host='120.25.103.83',user='root',passwd='Astarmo826@',db='trend',port=3306,charset='utf8') #5为连接
# pool = PooledDB(MySQLdb,5,host='localhost',user='root',passwd='123456',db='trend',port=3306,charset='utf8') #5为连接

"""
下面是各种工具方法
"""
# 存储爬取的主要数据
def saveTrendBasis(item):
    conn = pool.connection()
    cur = conn.cursor() # 获取游标
    sql = '''
        insert into trend_basis(addTime,deleteStatus,
        recruitUrl,title,companyName,companyUrl,salary,
        area,releaseDate,wordNature,workExperience,edu,
        recruitNum,field,companyScale,companyNature,companyIndustry,
        companyAddress)
        values(now(),0,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''

    # for each in item:
    #     print each

    cur.execute(sql, item)

    # cur.execute(sql,(item[0],item[1],item[2],item[3],item[4],
    #                  item[5],item[6],item[7],item[8],item[9],
    #                  item[10],item[11],item[12],item[13],item[14],
    #                  item[15]))

    cur.close()
    conn.commit()
    conn.close()

# 从数据库获得所有的小类代码和其父级分类代码
def getClassAndCode():
    conn = pool.connection();
    cur = conn.cursor()
    sql = '''
        select
          f1.field_number as sj,
          (select
            field_number
          from
            trend_field f2
          where f2.id = f1.parent_id) as bj
        from
          trend_field f1
        where f1.level = 3
    '''
    # and f1.field_number in(2040,43)

    # 获取总条数
    count = cur.execute(sql)

    # 查询出所有的结果
    result = cur.fetchmany(count)

    cur.close()
    conn.commit()
    conn.close()

    return result

# 通过 title 和 公司名称 判断数据库是否存在信息
def isBasis(title,companyName):
    conn = pool.connection()
    cur = conn.cursor()

    sql = "select * from trend_basis where title=%s and companyName = %s"
    count = cur.execute(sql, (title,companyName))
    cur.close()
    conn.commit()
    conn.close()
    if(count > 0):
        return 'have'
    else:
        return 'havenot'

# 通过 url 判断是否重复
def isBasisByUrl(url):
    conn = pool.connection()
    cur = conn.cursor()

    sql = "select * from trend_basis where recruitUrl=" + "'" + url + "'"
    count = cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()
    if (count > 0):
        return 'have'
    else:
        return 'havenot'

# 把所有需要处理的url存入trend_url中.
def saveAllUrl(url):

    #将 url 插入 trend_url 之前需要先检查一下 trend_url 中是否已存在该url,如果已经存在则不要插入了
    conn = pool.connection()
    cur = conn.cursor()
    sql = 'select * from trend_url where 1=%s and recruitUrl=%s'
    count = cur.execute(sql,(1,url))
    cur.close()
    conn.commit()
    conn.close()

    if(count == 0):
        # insert
        conn = pool.connection()
        cur = conn.cursor()

        status = 0
        sql = "insert into trend_url(addTime,deleteStatus,recruitUrl,status) values(now(),0,%s,%s)"
        cur.execute(sql, (url, status))
        cur.close()
        conn.commit()
        conn.close()

# 判断 trend_basis 中有没有某个 url
def judgeUrlBasis(url):
    conn = pool.connection()
    cur = conn.cursor()

    sql = 'select * from trend_basis where recruitUrl=' + '"' + url + '"'
    count = cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()
    return count

# 获取 trend_url 中status为0的数据
def getUrlBystatus():
    conn = pool.connection()
    cur = conn.cursor()

    sql = 'select id,recruitUrl from trend_url where status=0'
    count = cur.execute(sql)
    results = cur.fetchmany(count)
    cur.close()
    conn.commit()
    conn.close()

    return results

# 获取 trend_url 中status为0的数据(单机专用)
def getUrlBystatus_dell():
    conn = pool.connection()
    cur = conn.cursor()

    sql = 'select id,recruitUrl from trend_url where status=0 order by addTime desc'
    count = cur.execute(sql)
    results = cur.fetchmany(count)
    cur.close()
    conn.commit()
    conn.close()

    return results

# 把trend_url中的某条数据的status置为1 输入:id
def setStatus(id):
    conn = pool.connection()
    cur = conn.cursor()

    sql = 'update trend_url set status=%s where id=%s'
    cur.execute(sql, (1, id))
    cur.close()
    conn.commit()
    conn.close()

# 把trend_url中的某条数据的status置为1 输入: recruitUrl
def setStatusByUrl(recruitUrl):
    conn = pool.connection()
    cur = conn.cursor()

    sql = 'update trend_url set status=%s where recruitUrl=%s'
    cur.execute(sql, (1, recruitUrl))
    cur.close()
    conn.commit()
    conn.close()

# 删除 trend_url 中不合格的 url
def deleteTrendUrl():
    conn = pool.connection()
    cur = conn.cursor()

    sql = 'delete from trend_url where recruitUrl not like "%http://jobs.zhaopin.com%"'
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()

'''
获取七个大类和所有小类
先存储七个大类,然后在存储所有小类,在小类上加上大类的parent_id
'''

# 获取顶级分类-并存储到数据库
def getTopClass():
    html = htmllist.field0
    classes = re.findall('<td class="leftClass jobtypeLCla" nowrap="nowrap" valign="middle">(.*?)</td>', html, re.S)
    cla = classes[0]
    cla = cla.strip()

    #存储数据库
    conn= MySQLdb.connect(host='localhost',port = 3306,user='root',passwd='123456',db ='trend',chartset='utf8')
    cur = conn.cursor() # 获得游标
    sqli = "insert into trend_field(level,field_name,common) values(%s,%s,%s)"
    cur.execute(sqli,(1, cla, 1))
    print cla
    cur.close()
    conn.commit()
    conn.close()

#获取七个大类-并存储到数据库
def getBigClasses():
    # 网址
    html = htmllist.field0;
    # 声明数组存放类名和分类
    classes = []
    # 类名 # 分类代码
    cls = re.findall('fnPopupChildren(.*?)">', html, re.S)
    for each in cls:
        #类名
        cal = []
        e = re.findall(",'(.*?)']",each,re.S)
        cal.append(e[0])
        # 分类代码
        e = re.findall('(\d+)',each, re.S)
        cal.append(e[0])
        classes.append(cal)

    # 存储到数据库
    for each in classes:
        # print each[0]
        # print each[1]
        conn = pool.connection()
        cur = conn.cursor()  # 获得游标
        sql = "insert into trend_field(level,field_name,field_number,parent_id) values(%s,%s,%s,%s)"
        cur.execute(sql, (2, each[0],each[1], 8))
        cur.close()
        conn.commit()
        conn.close()

# 获取小类代码和名称 / 需要手动调整1-7
def getSmallClasses():
    html = htmllist.field7
    cods = re.findall('value="(\d+)"', html, re.S)
    cls = re.findall('iname="(.*?)"', html, re.S)
    for each in range(0, len(cls)-1):
        conn = pool.connection()
        cur = conn.cursor()
        sql = "insert into trend_field(level,field_name,field_number,parent_id) values(%s,%s,%s,%s)"
        cur.execute(sql,(3,cls[each],cods[each],7))
        print cls[each]
        cur.close()
        conn.commit()
        conn.close()

if __name__ == '__main__':
    print '测试'
    conn = pool.connection();
    print conn

