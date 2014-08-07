#!/usr/bin/perl
##########################################################################
# (c) Copyright Snug sang. 2010 All rights reserved.
#  SourceName   :  DB2Util.pm
#  Description  :  Defines common functions like command line argument checking 
#             		 Also, functions to prepare and execute an SQL statement, and 
#            		   roll back if an error occurs.etc.
#  Author       �� Snug sang   <shiren1118@gmail.com>
#  Create_time  :  2010-03-15
#  Modify_time  :  2010-03-17
#
#	���ڱ����� 50 ������ʼ��ĳ���ڱ�����ֻ��Ҫ�趨��ǰ���ڣ�		 ������ʼ���������ڱ��� ETL::initDate($TX_DATE);��ETL�ı�׼ģ�嵹����5�� 
#								 
#  ʹ�����ڱ��� :
#
#          һ����ETL�ı�׼ģ����
#							my $sql = "
#                              select 
#                                   *
#                              from table
#                              where date_stamp = '$ETL::D_NEXT1MONTH_FIRSTDAY'
#								   ";	 
#                  �ڽű���ֱ��ʹ�ü��ɣ�ע������ˡ�$ETL::����
#								 
#          �����ڷ�ETL�ı�׼ģ����
#							�������ETL�ı�׼ģ�壬��Ҫ���г�ʼ��
#         						1��	setD_DATE_TODAY(CharToDate($_[0]));
#         						2��	sgetDATE_TODAY_L1();
#							�����Ϳ���ʹ��# $ETL::DATE_TODAY_L1��������ˣ�ʹ�÷���ͬ�ϡ�
#
#
#
#  �����ຯ��   ��
#                      -getTimeBy               ����ʱ�����ͣ��ӵ�ǰʱ���л�ȡֵ�����赱ǰ����Ϊ2010-03-17 ���ָ������Ϊday���򷵻�ֵΪ 17 
#	                          								���ܲ����б�year ��mon ��day hour minute  second  weekOfDay earOfDay
#
#
#  Example      ��   							     	my $myDay=ETL::getTimeBy("hour");
#  ����Ӧ��     ��  ��createTask.pl�д˷���ʾ��
#
#
#
#
#
#
#             
# ���ڱ����б�
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
# Description : �ֶ���ʼ��ָ������
# Input       : None
# Output      : print
###########################################################################
sub unInitDate
{
  #��ETL�ı�׼ģ�嵹����5�� ����ע��
	
	#ע��setD_DATE_TODAY�������ڸ�ʽ2010-03-17��   �滻����setD_DATE_TODAY(20100317)��
  ETL::setD_DATE_TODAY(ETL::CharToDate(   $today ) );
	
	#set date
	ETL::sgetDATE_TODAY_L1();
	
	print  "�ֶ���ʼ���ڱ���ֵ�ǣ�".$ETL::DATE_TODAY_L1;
}
 
 
##########################################################################
# Description : �Զ���ʼ��
# Input       : None
# Output      : ��mk_vsdm.a�ļ�¼����  
###########################################################################
sub initDate{

  #��ETL�ı�׼ģ�嵹����5�� ����ע��
	
  #��ʼ���������ڱ���,ע��������ڸ�ʽ20100317
	ETL::initDate($today);
	
	
	#��ʽ�ű�������ע�͡�
	#����cmdִ��bat�ű�
	#ETL::run_cmd($bat);
	
	print  "�Զ���ʼ���ڱ���ֵDATE_TODAY_L1�ǣ�".$ETL::DATE_TODAY_L1."\n";
	print  "�Զ���ʼ���ڱ���ֵDATE_TODAY_L2�ǣ�".$ETL::DATE_TODAY_L2."\n";
	
}

  
	
##########################################################################
# Description : ���������ű���ں���
# Input       : None
# Output      : None 
###########################################################################
sub main
{

  #����2���������ֱ�ִ��
	
	#�������unInitDate���Ͱ�initDateע�͵���Ȼ������
  #unInitDate();
	initDate();

	return 0;
}

#----˵����Ϊ�˲��Է��㣬����ʽ�ű��е����浹�������м���ע��

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
#----����ʽ�ű��������зſ�ע��
# ETL::initDate($TX_DATE);
my $TX_MONTH=substr($TX_DATE,0,6);
open(STDERR, ">&STDOUT");
my $ret = main();
exit($ret);



 



