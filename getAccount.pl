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
#
# 		  			-getAllCount              获得表中记录总数，参数是表名
# 		  			-getCountBySQL            通过SQL获得表中记录总数，参数是SQL，如“select count(0) from mk_vsdm.a”返回的是数组的第一个，所以不要select多个字段
#
#
#  Execute      :  perl getAccount.pl 20100318
#
#
##########################################################################
select STDERR; $|=1;
select STDOUT; $|=1;

use DBI;
use strict;
use warnings; 


# access the module for DB2  Utility functions
use DB2Util;



##########################################################################
# Description : 获得表mk_vsdm.a的记录总数
# Input       : None
# Output      : 表mk_vsdm.a的记录总数  
###########################################################################
sub getAccount{
	#sql语句
  my $sql = "select count(0) from mk_vsdm.a";
	
	#执行并获得返回的数值
	my @aaa = ETL::getSelect( $sql )->fetchrow();
	
	return $aaa[0];
}

  
 
  
##########################################################################
# Description : 主方法，脚本入口函数
# Input       : None
# Output      : None 
###########################################################################
sub main
{
  #查看当前表中的记录数
  ETL::mylog( "my sub  :".getAccount());
	
	#提取表中所有记录总数
	ETL::mylog(  "ETL::getAllCount sub   :".ETL::getAllCount("mk_vsdm.a"));
	
	#通过sql来提取表中所有记录数
	ETL::mylog( "ETL::getCountBySQL  sub  :".ETL::getCountBySQL("select count(0) from mk_vsdm.a"));
	
	#关闭连接
	ETL::disconnect();
	
	return 0;
}


######################################################################
# program section

# To see if there is one parameter,
# if there is no parameter, exit program
if ( $#ARGV < 0 )
{
   exit(1);
}
# Get the first argument
my $CONTROL_FILE = $ARGV[0];

my $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 8);
if ( substr(${CONTROL_FILE}, length(${CONTROL_FILE})-3, 3) eq 'dir' ) {
    $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 8);
};
# init date variables
ETL::initDate($TX_DATE);
my $TX_MONTH=substr($TX_DATE,0,6);
open(STDERR, ">&STDOUT");
my $ret = main();
exit($ret);



 



