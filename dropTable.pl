#!/usr/bin/perl
##########################################################################
# (c) Copyright Snug sang. 2010 All rights reserved.
#  SourceName   :  DB2Util.pm
#  Description  :  �˽ű����ڲ���dropTables()����
#             		 
#            		 
#  Author       �� Snug sang   <shiren1118@gmail.com>
#  Create_time  :  2010-03-15
#  Modify_time  :  2010-03-17
#              update : ����������������
#
# ˵��           :  dropTables() �Ĳ�����һ���ַ������飬���������޶����ֻҪ�Ǳ�������
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
# Description : ��ȡ���м�¼����while���д���
# Input       : None
# Output      : ��mk_vsdm.a�ļ�¼����  
###########################################################################
sub createTable{

	ETL::mySubLog();
	#sql���
  my $sql = " CREATE TABLE MK_VSDM.TEST11 (
								ID	INTEGER
							) ;
							CREATE TABLE MK_VSDM.TESTA1 LIKE MK_VSDM.TEST11;
							CREATE TABLE MK_VSDM.TESTA2 LIKE MK_VSDM.TEST11;
					";
	
	#ִ��
	ETL::run_db2cmd( $sql ) ; 

}

 
  
##########################################################################
# Description : ���������ű���ں���
# Input       : None
# Output      : None 
###########################################################################
sub main
{
  #�鿴��ǰ���еļ�¼��
  createTable();
	
	ETL::dropTables("MK_VSDM.TEST11");
 
	ETL::dropTables("MK_VSDM.TESTA1","MK_VSDM.TESTA2");
	
	print "ssss\n";
	
	ETL::dropTables("MK_VSDM.TEST33");

	
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
# init date variables,�˴���ʼ����DBI::DBD����
#���Ҫ���Բ�����DBI::DBD����ʱ�ܷ�ɹ�����db2cmd��ִ�У��ɽ�����һ�д������ע�ͣ�Ȼ������ִ��
ETL::initDate($TX_DATE);
my $TX_MONTH=substr($TX_DATE,0,6);
open(STDERR, ">&STDOUT");
my $ret = main();
exit($ret);



 



