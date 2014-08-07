 #!/usr/bin/perl
##########################################################################
# (c) Copyright Snug sang. 2010 All rights reserved.
#  SourceName   :  DB2Util.pm
#  Description  :  此脚本用于测试run_db2_drop()方法
#             		 
#            		 
#  Author       ： Snug sang   <shiren1118@gmail.com>
#  Create_time  :  2010-03-15
#  Modify_time  :  2010-03-17
#              update : 修正共享连接问题
#
# 说明           :   isDb2TableExist()  参数是 模式名+ 表名  的字符串
#                   
#  Execute      :  perl  dropTable.pl 20100318
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
# Description : 获取表中记录，用while进行处理
# Input       : None
# Output      : 表mk_vsdm.a的记录总数  
###########################################################################
sub createTable{

	ETL::mySubLog();
	#sql语句
  my $sql = " CREATE TABLE MK_VSDM.TEST11 (
								ID	INTEGER
							) ;

					";
	
	#执行
	ETL::run_db2cmd( $sql ) ; 

}

 
  
##########################################################################
# Description : 主方法，脚本入口函数
# Input       : None
# Output      : None 
###########################################################################
sub main
{
  #查看当前表中的记录数
  createTable();
	
	print ETL::isDb2TableExist("MK_VSDM.TEST11");
	
	ETL::dropTables("MK_VSDM.TEST11");
	
	print ETL::isDb2TableExist("MK_VSDM.TEST11");
	
  

	
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
# init date variables,此处初始化了DBI::DBD连接
#如果要测试不存在DBI::DBD连接时能否成功调用db2cmd来执行，可将下面一行代码进行注释，然后重新执行
ETL::initDate($TX_DATE);
my $TX_MONTH=substr($TX_DATE,0,6);
open(STDERR, ">&STDOUT");
my $ret = main();
exit($ret);



 



