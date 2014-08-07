#!/usr/bin/perl
##########################################################################
# (c) Copyright Snug sang. 2010 All rights reserved.
#  SourceName   :  DB2Util.pm
#  Description  :  Defines common functions like command line argument checking 
#             		 Also, functions to prepare and execute an SQL statement, and 
#            		   roll back if an error occurs.etc.
#  Author       ： Snug sang   <shiren1118@gmail.com>
#  Create_time  :  2010-03-15
#  Modify_time  :  2010-03-17
#
#	日期变量共 50 个，初始化某日期变量，只需要设定当前日期，		 如果想初始化所有日期变量 ETL::initDate($TX_DATE);在ETL的标准模板倒数第5行 
#								 
#  使用日期变量 :
#
#          一、在ETL的标准模板中
#							my $sql = "
#                              select 
#                                   *
#                              from table
#                              where date_stamp = '$ETL::D_NEXT1MONTH_FIRSTDAY'
#								   ";	 
#                  在脚本中直接使用即可，注意别忘了“$ETL::”。
#								 
#          二、在非ETL的标准模板中
#							如果不是ETL的标准模板，需要自行初始化
#         						1、	setD_DATE_TODAY(CharToDate($_[0]));
#         						2、	sgetDATE_TODAY_L1();
#							这样就可以使用# $ETL::DATE_TODAY_L1这个变量了，使用方法同上。
#
#
#
#  日期类函数   ：
#                      -getTimeBy               根据时间类型，从当前时间中获取值，假设当前日期为2010-03-17 如果指定参数为day，则返回值为 17 
#	                          								可能参数列表：year ，mon ，day hour minute  second  weekOfDay earOfDay
#
#
#  Example      ：   							     	my $myDay=ETL::getTimeBy("hour");
#  典型应用     ：  见createTask.pl中此方法示例
#
#
#
#
#
#
#             
# 日期变量列表
# $ETL::D_NEXT1MONTH_FIRSTDAY
# $ETL::D_NEXT1MONTH_LASTDAY
# $ETL::D_NEXT1MONTH_TODAY
# $ETL::D_DATE_TODAY
# $ETL::D_MONTH_FIRSTDAY
# $ETL::D_MONTH_LASTDAY
# $ETL::D_LAST1MONTH_FIRSTDAY
# $ETL::D_LAST1MONTH_LASTDAY
# $ETL::D_LAST1MONTH_TODAY
# $ETL::D_LAST2MONTH_FIRSTDAY
# $ETL::D_LAST2MONTH_LASTDAY
# $ETL::D_LAST2MONTH_TODAY
# $ETL::D_LAST3MONTH_FIRSTDAY
# $ETL::D_LAST3MONTH_LASTDAY
# $ETL::D_LAST3MONTH_TODAY
# $ETL::NEXT1MONTH_FIRSTDAY
# $ETL::NEXT1MONTH_LASTDAY
# $ETL::NEXT1MONTH_TODAY
# $ETL::NEXT1MONTH_CHAR  
# $ETL::DATE_TODAY
# $ETL::MONTH_FIRSTDAY
# $ETL::MONTH_LASTDAY
# $ETL::MONTH_CHAR  
# $ETL::LAST1MONTH_FIRSTDAY
# $ETL::LAST1MONTH_LASTDAY
# $ETL::LAST1MONTH_TODAY
# $ETL::LAST1MONTH_CHAR  
# $ETL::LAST2MONTH_FIRSTDAY
# $ETL::LAST2MONTH_LASTDAY
# $ETL::LAST2MONTH_TODAY
# $ETL::LAST2MONTH_CHAR  
# $ETL::LAST3MONTH_FIRSTDAY
# $ETL::LAST3MONTH_LASTDAY
# $ETL::LAST3MONTH_TODAY
# $ETL::LAST3MONTH_CHAR  
# $ETL::D_DATE_TODAY_L1
# $ETL::D_DATE_TODAY_L2
# $ETL::D_DATE_TODAY_L3
# $ETL::D_DATE_TODAY_L4
# $ETL::D_DATE_TODAY_L5
# $ETL::D_DATE_TODAY_L6
# $ETL::DATE_TODAY_L1
# $ETL::DATE_TODAY_L2
# $ETL::DATE_TODAY_L3
# $ETL::DATE_TODAY_L4
# $ETL::DATE_TODAY_L5
# $ETL::DATE_TODAY_L6
# $ETL::WEEK_OF_CALENDAR
# $ETL::MONTH_OF_CALENDAR
# $ETL::ISSUNDAY_FLAG  
#
#  Execute      :  perl  run_db2cmd.pl  20100318
##########################################################################
select STDERR; $|=1;
select STDOUT; $|=1;

use DBI;
use strict;
use warnings; 


# access the module for DB2  Utility functions
use DB2Util;
 
 
 #declare variable
 
 my $today;
 
 
###########################################################################
# Description : 手动初始化指定日期
# Input       : None
# Output      : print
###########################################################################
sub unInitDate
{
  #在ETL的标准模板倒数第5行 加了注释
	
	#注意setD_DATE_TODAY参数日期格式2010-03-17，   替换方案setD_DATE_TODAY(20100317)；
  ETL::setD_DATE_TODAY(ETL::CharToDate(   $today ) );
	
	#set date
	ETL::sgetDATE_TODAY_L1();
	
	print  "手动初始日期变量值是：".$ETL::DATE_TODAY_L1;
}
 
 
##########################################################################
# Description : 自动初始化
# Input       : None
# Output      : 表mk_vsdm.a的记录总数  
###########################################################################
sub initDate{

  #在ETL的标准模板倒数第5行 加了注释
	
  #初始化所有日期变量,注意参数日期格式20100317
	ETL::initDate($today);
	
	
	#正式脚本打开下面注释。
	#调用cmd执行bat脚本
	#ETL::run_cmd($bat);
	
	print  "自动初始日期变量值DATE_TODAY_L1是：".$ETL::DATE_TODAY_L1."\n";
	print  "自动初始日期变量值DATE_TODAY_L2是：".$ETL::DATE_TODAY_L2."\n";
	
}

  
	
##########################################################################
# Description : 主方法，脚本入口函数
# Input       : None
# Output      : None 
###########################################################################
sub main
{

  #下面2个方法，分别执行
	
	#如果测试unInitDate，就把initDate注释掉，然后运行
  #unInitDate();
	initDate();

	return 0;
}

#----说明：为了测试方便，把正式脚本中的下面倒数第五行加了注释

######################################################################
# program section

# To see if there is one parameter,
# if there is no parameter, exit program
if ( $#ARGV < 0 )
{
   exit(1);
}
# # Get the first argument
my $CONTROL_FILE = $ARGV[0];

my $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 8);
if ( substr(${CONTROL_FILE}, length(${CONTROL_FILE})-3, 3) eq 'dir' ) {
    $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 8);
};
$today = $TX_DATE;
# # init date variables
#----在正式脚本中请自行放开注释
# ETL::initDate($TX_DATE);
my $TX_MONTH=substr($TX_DATE,0,6);
open(STDERR, ">&STDOUT");
my $ret = main();
exit($ret);



 



