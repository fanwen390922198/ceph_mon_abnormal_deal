#!/usr/bin/python
# -*- coding:UTF-8 -*-
'''
	Copyright (C) 2018 - All Rights Reserved
	所属工程: ceph_test_analysis
	模块名称: big_file.py
	创建日期: 2018/9/14 9:28
	代码编写: fanwen
	功能说明: 
'''
import datetime
import random
import os

from multiprocessing import Process,Queue,Lock
import multiprocessing

import time

#计算时间函数
def print_run_time(func):
	def wrapper(*args, **kw):
		local_time = time.time()
		ret = func(*args, **kw)
		print 'current Function [%s] run time is %.2f' % (func.__name__ ,time.time() - local_time)
		return ret;
	return wrapper


__CHUNK_SIZE__ = 1024*1024*100; # 大文件分片时，每个分片大小
__CHUNK_FILE_PATH__ = './split';
# __CHUNK_SIZE__ = 1024*1024*1; # 大文件分片时，每个分片大小

# @print_run_time
# def  MakeBigFile():
# 	hf = open("./ceph.log", "a+");
# 	ss = "%s osd.%d [WRN] slow request 3.006348 seconds old, received at 2018-09-10 18:19:58.430351: osd_op(client.3631771.0:28536 rb.0.376a8c.238e1f29.00000000083a [set-alloc-hint object_size 4194304 write_size 4194304,write 1335296~4096] 10.84d8876c ack+ondisk+write+known_if_redirected e47671) currently commit_sent\n";
# 	for i in range(0, 4000):
# 		sw = "";
# 		for j in range(0, 1000):
# 			sw += ss%(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), random.randint(0,200));
# 		hf.write(sw);

# 	hf.close();

#----------------------------------------------------------------------#
# 结果分析基类，不同种类的数据请继承此类，并实现parse_key_in_line 和
# deal_key_to_res_data
#----------------------------------------------------------------------#
class ResultAnalysis:
    def __init__(self, chunk, in_file = False):
        self.res_data = {};   # 最终会形成一个结果集
        self.chunks = chunk;
        self.in_file = in_file;

    def parse_key_in_line(self, str_line):
        """
        在一行中获取关键字，不管是使用正则表达式，还是字符串分析
        :return:  结果请以列表的形式返回
        """
        return [];

    def deal_key_to_res_data(self, line_key = []):
        """
        将一行中的关键字，进行处理，并将处理结果添加到结果集中
        :return:
        """
        pass;

    def parse_chunk_in_file(self):
        """
        通过文件分片来处理
        :return:
        """
        with open(self.chunks, "r+") as f:
            for line in f:
                line_key = self.parse_key_in_line(line);
                if len(line_key) == 0: continue;

                self.deal_key_to_res_data(line_key);

    def parse_chunk_in_memory(self):
        """
        通过内存分片来处理
        :return:
        """
        for line in self.chunks.split('\n'):
            line_key = self.parse_key_in_line(line);
            if len(line_key) == 0: continue;

            self.deal_key_to_res_data(line_key);

#-------------------------------------------------------------------------------------
#-- 进程函数
#-------------------------------------------------------------------------------------
def process_proc(chunk, queue, in_file, ResultAnalysisClass):
    proc_name = multiprocessing.current_process().name;
    ra = ResultAnalysisClass(chunk, in_file);
    if in_file:
        ra.parse_chunk_in_file();
    else:
        ra.parse_chunk_in_memory();

    print "process[%s] start to put queue size：%d"%(proc_name, len(str(ra.res_data)));
    queue.put(str(ra.res_data));
    print "process[%s] complete put queue size：%d\r\n"%(proc_name, len(str(ra.res_data)));
    return 0;


#-------------------------------------------------------------------------------------
#-- 大文件分析模块
#-------------------------------------------------------------------------------------
class BigFileAnalysis:
    def __init__(self, file_path, in_file = False, ResultAnalysisClass=ResultAnalysis):
        self.file_path = file_path;
        self.in_file = in_file;         #-- 是否使用内存处理(或者通过文件来处理分片)
        self.chunks = [];  # 获取的文件分片
        self.file_size = 0;
        self.result_analysis_class = ResultAnalysisClass;

        if self.in_file == True:
            try:
                file_dir = os.path.split(file_path);
                self.chunks_file_dir = "%s/split/"%file_dir[0];   # -- 创建存放文件分片的目录
                if os.path.exists(self.chunks_file_dir):
                    all_ = os.listdir(self.chunks_file_dir);
                    for f_txt in all_:
                        fp = os.path.join(self.chunks_file_dir, f_txt);
                        os.remove(fp);
                else:
                    os.mkdir(self.chunks_file_dir);
            except IOError as e:
                raise e;

    def __del__(self):
        pass;

    @print_run_time
    def split_big_file(self):
        try:
            hf = open(self.file_path, "r");
            hf.seek(0, os.SEEK_END);
            self.file_size = hf.tell();
            hf.seek(0);
            print "file_size: ", self.file_size;
        except IOError as e:
            print e.message;
            return;
        # finally:

        if self.file_size < __CHUNK_SIZE__:
            self.in_file = False;      # -- 如果文件不够一个分片的话，那么就直接在内存中处理
            self.chunks.append(hf.read(self.file_size));
        else:
            extra_size = self.file_size;
            i = 0;
            while extra_size > 0:
                read_size = __CHUNK_SIZE__ if extra_size > __CHUNK_SIZE__ else extra_size;
                ss = hf.read(read_size); # 事实证明当从一个大文件中读一个200M的数据，大概需要2s(当然这得看机器的配置了)
                if ss[-1] != '\n':
                    ex = hf.readline(); # 如果刚好某一行被截断，使用readline 从当前文件指针位置可直接读到下一行；
                    ss += ex;

                if self.in_file:  # -- 使用文件分片
                    self.chunks.append("%sdata_%d.log" %(self.chunks_file_dir, i));
                    hf_s = open("%sdata_%d.log" %(self.chunks_file_dir, i), "a+");
                    hf_s.write(ss);
                    hf_s.close();
                    i += 1;
                else:
                    self.chunks.append(ss);  # -- 使用内存分片，避免后续的分析再次读写文件(前提是你有足够的内存可用)

                extra_size -= read_size;

            hf.close();

    @print_run_time
    def start_analusis_res(self):
        """
        分析结果
        :return:
        """
        queue = Queue(maxsize=1024*1024*100);   # 必须设置queue的大小
        proc_list = [];

        for i in range(0, len(self.chunks)):
            p = Process(name="process_%d"%i, target=process_proc,
                        args=(self.chunks[i], queue, self.in_file, self.result_analysis_class,));
            proc_list.append(p);

        for process in proc_list:
            process.start();

        #-- 必须不停的获取queue中的数据，否则将导致队列满进程被阻塞
        while True:
            if queue.empty():
                time.sleep(0.01);
            else:
                print "get queue: ", len(queue.get());

            all_process_not_exit = len(proc_list);
            for process in proc_list:
                if not process.is_alive():
                    all_process_not_exit -= 1;

            if all_process_not_exit == 0:
                break;

        # queue.close();

        for process in proc_list:
            process.join();
