#!/usr/bin/python
# -*- coding:UTF-8 -*-
'''
	Copyright (C) 2018 - All Rights Reserved
	所属工程: ceph
	模块名称: spilit_file2.py
	创建日期: 2019/7/15 13:00
	代码编写: fanwen
	功能说明: 
'''
import datetime
import os
import re
import time

__CHUNK_SIZE__ = 1024*1024*30   #大文件分片时，每个分片大小
ABNORMAL_LINE_LEN = 300  # 异常长度


#计算时间函数
def print_run_time(func):
	def wrapper(*args, **kw):
		local_time = time.time()
		ret = func(*args, **kw)
		print 'current Function [%s] run time is %.2f' % (func.__name__ ,time.time() - local_time)
		return ret
	return wrapper

def Find_abnormal_osd(slog, abnormal_osd = []):
    pattern = re.compile(r'(osd.\d+)')
    all_find = pattern.findall(slog)
    abnormal_osd.extend(all_find)

@print_run_time
def BigFileSplit(file_path, deal_size = -1):
    """
    :param FilePath:    大文件路径
    :param deal_size:   只处理的大小(从头到尾), 如果没有指定则处理整个文件
    :return: 
    """

    chunk_size = __CHUNK_SIZE__
    abnormal_osd = []
    abnormal_osd_slowrequest_count = {}

    #获取文件大小(byte)
    try:
        hf = open(file_path, "r")
        hf.seek(0, os.SEEK_END)
        size = hf.tell()
        hf.seek(0)
        print "日志文件长度: %d", size
    except IOError as e:
        print e.message
        return

    extra_size = size
    while extra_size > 0:
        read_size = chunk_size if extra_size > chunk_size else extra_size
        local_time = time.time()
        ss = hf.read(read_size)
        print 'read run time is %.2f' % (time.time() - local_time)

        # 如果刚好某一行被截断，使用readline 从当前文件指针位置可直接读到下一行;
        if ss[-1] != '\n':
            ex = hf.readline() #读取被截断的行剩余部分
            if len(ex) > ABNORMAL_LINE_LEN:  #遇到乱码
                # extra_size -= (read_size + len(ex))
                print "异常日志，乱码长度:%d, 将被丢弃"%(len(ex))
            else:
                ss += ex

        # 获取异常日志osd
        Find_abnormal_osd(ss, abnormal_osd)

        end_pos = hf.tell()
        extra_size = size - end_pos

    hf.close()

    osd_set = set(abnormal_osd)
    for osd in osd_set:
        abnormal_osd_slowrequest_count[osd] = abnormal_osd.count(osd)

    all_abnornaml_osd = sorted(abnormal_osd_slowrequest_count.items(),key = lambda x:x[1],reverse = True)
    for ab_osd in all_abnornaml_osd:
        print "%s发送Slow Request日志%d条!"%(ab_osd[0], ab_osd[1])

if __name__ == "__main__":
    BigFileSplit('/root/ceph.log.bak', deal_size = -1)
