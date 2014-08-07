#!/usr/bin/perl
##########################################################################
# (c) Copyright Snug sang. 2010 All rights reserved.
#  SourceName   :  DB2Util.pm
#  Description  :  Defines common functions like command line argument checking 
#             		 Also, functions to prepare and execute an SQL statement, and 
#            		   roll back if an error occurs.etc.
#  Author       ： Snug sang   <shrien1118@gmail.com>
#  Create_time  :  2010-03-15
#  Modify_time  :  2010-03-17
#
#								写好一个bat，放到一个sub方法中，然后在main方法中写一个while死循环	 
#								 检测日期equal 某个日期（年月日时分秒）
#
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
 


sub callTask{

  #execute E:\aaa\test.bat
	my $bat=" D:
           cd qw
           call a.bat 
        ";
  
	print $bat;
	
	#正式脚本打开下面注释。
	#调用cmd执行bat脚本
	ETL::run_cmd($bat);
}

  
	
	
sub main
{
  #日期变量
	my $myDay="";
	
	#计数器变量
	my $account=0;
	
	while(1){
		#可能参数列表：
		#year
		#mon
		#day
		#hour
		#minute
		#second
		#weekOfDay
		#yearOfDay
		$myDay=ETL::getTimeBy("hour");
		$account = $account + 1 ;
		 
		sleep(10);
		
		#这里是15点调用callTask()方法，可以在if条件里加15点40分22秒···
		if($myDay eq "15"){
			#方法日志
		  ETL::mySubLog();
			
			#普通日志
		  ETL::mylog("第  $account 次 调用callTask()");
			
			#核心方法，调用bat文件
		  callTask();
			
			#方法日志
			ETL::mySubLog();
		}
		
		#编写退出while条件，即什么时候停止调用
		if($myDay eq "16"){
		
			ETL::mylog("调用callTask() finished");
			
			exit(0);
			
		}
	  
	
	}

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



 



