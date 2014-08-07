#!/usr/bin/perl
##########################################################################
# (c) Copyright Snug sang. 2010 All rights reserved.
#  SourceName   :  DB2Util.pm
#  Description  :  Defines common functions like command line argument checking 
#             		 Also, functions to prepare and execute an SQL statement, and 
#            		   roll back if an error occurs.etc.
#  Author       ： Snug sang  <shiren1118@gmail.com>
#  Create_time  :  2010-03-15
#  Modify_time  :  2010-03-17
#              update : 修正共享连接问题
#
#
#
#  Execute      :  perl  getSelect.pl 20100318
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
sub printAll{

	ETL::mySubLog();
	#sql语句
  my $sql = " select
									measure_id
									,measure_name
									,serv_id
									,auto_hand_flag 
							FROM mk_vgop.TB_DIM_AUTO_HAND_MEASURE 
							WHERE AUTO_HAND_FLAG='H' and SERV_ID='S010'
					";
	
	#执行并获得返回的数值
	my @aaa ;
	my $sth = ETL::getSelect( $sql ) ; 
	
	while(@aaa = $sth->fetchrow()){ 
	
		my $measure_id = $aaa[0];
		my $measure_name = $aaa[1] ;
		my $serv_id = $aaa[2];
		my $auto_hand_flag = $aaa[3] ;
		
		print $measure_id."   -----   ".$measure_name."\n"
	}
	
	# return $aaa[0];
}

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
  print getAccount();
	
	#sql语句，采用的是db2cmd的方式，
  my $sql=" insert into mk_vsdm.a values(13);
						insert into mk_vsdm.a values(13);
						insert into mk_vsdm.a values(13);
						insert into mk_vsdm.a values(13);
						insert into mk_vsdm.a values(13);
						insert into mk_vsdm.a values(13);
				";
	ETL::run_db2cmd($sql);
	
	#查看插入记录后的当前表中的记录数
	print getAccount();
	
	
	printAll();
	
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



 



