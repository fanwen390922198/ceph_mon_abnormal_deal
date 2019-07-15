#!/usr/bin/python
# -*- coding:UTF-8 -*-
'''
	Copyright (C) 2018 - All Rights Reserved
	所属工程: ceph_test_analysis
	模块名称: slow_request_analysis.py
	创建日期: 2018/9/14 9:34
	代码编写: fanwen
	功能说明: 
'''

from base_lib import print_run_time
from big_file import ResultAnalysis

# 正则表达式使用效率很低，跟自己写的规则有关
# pattern = re.compile(r'(.{26}) (osd.+) \[WRN\] slow request (.+) seconds old, .*?(osd_.+)\(.*?(rb.{30})');
# with open(FilePath,"r+") as f:
# 	for line in f:
# 		pattern.findall(line);

def	MergeSlowRequstData(d1, d2):
	for osd in d2.keys():
		if not d1.has_key(osd):
			d1[osd] = d2[osd];
		else:
			for obj in d2[osd].keys():
				if not d1[osd].has_key(obj):
					d1[osd][obj] = d2[osd][obj];
				else:
					d1[osd][obj].extend(d2[osd][obj]);

def  SlowRequestAnalysis(slow_request_data = {}):
	top = [];

	for osd in slow_request_data.keys():
		osd_slow = slow_request_data[osd];
		count = 0;
		all_slow_requst_sec = 0;
		for obj in osd_slow.keys():
			count += len(osd_slow[obj]);
			all_slow_requst_sec += sum([eval(d[1]) for d in osd_slow[obj]]);

		avg = float(all_slow_requst_sec / count);
		top.append([osd, count, "%.3f"%avg]);

	# ll = sorted(top, key=lambda x: (x[8], eval(x[1])), reverse=True);
	ll = sorted(top, key=lambda x: x[1], reverse=True);


class SlowRequstAnalysis(ResultAnalysis):
    def __init__(self, chunk, in_file):
        #super(SlowRequstAnalysis, self).__init__(chunk, in_file);
        ResultAnalysis.__init__(self, chunk, in_file);

    def parse_key_in_line(self, str_line):
        """
        在一行中获取关键字，不管是使用正则表达式，还是字符串分析
        :return:  结果请以列表的形式返回
        """
        i = str_line.find("slow request");
        j = str_line.find("seconds old");
        if i != -1 and j != -1:
            secs = str_line[i + len("slow request") + 1: j - 1];
            try:
                n = str_line.find("osd.");
                osd = str_line[n: str_line.find("[WRN]") - 1];
                dtime = str_line[0: n - 1];

                if str_line.find("osd_op") != -1:
                    op = "osd_op";

                elif str_line.find("osd_repop") != -1:
                    op = "osd_repop";

                m = str_line.find("rb.");
                if m != -1:
                    object = str_line[m: m + 33];
                else:
                    m = str_line.find("rbd_data.");
                    if m != -1:
                        object = str_line[m: m + 39];
                    else:
                        object = "";
            except:
                pass;
            finally:
                return [dtime, osd, secs, op, object];

        return [];

    def deal_key_to_res_data(self, line_key = []):
        """
        将一行中的关键字，进行处理，并将处理结果添加到结果集中
        :return:
        """
        if not self.res_data.has_key(line_key[1]):
            self.res_data[line_key[1]] = {};

        if not self.res_data[line_key[1]].has_key(line_key[4]):
            self.res_data[line_key[1]][line_key[4]] = [];

        self.res_data[line_key[1]][line_key[4]].append([line_key[0], line_key[2], line_key[3]]);


if __name__ == "__main__":
    from big_file import BigFileAnalysis
    from big_file import MakeBigFile

    # MakeBigFile();
    bf = BigFileAnalysis("./ceph-osd.1.log", False, SlowRequstAnalysis);
    bf.split_big_file();
    bf.start_analusis_res();

