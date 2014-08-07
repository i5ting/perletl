#!/usr/bin/perl
##########################################################################
# (c) Copyright Snug sang. 2010 All rights reserved.
#  SourceName   :  DB2Util.pm
#  Description  :  Defines common functions like command line argument checking 
#             		 Also, functions to prepare and execute an SQL statement, and 
#            		   roll back if an error occurs.etc.
#  Author       ： sangshilong   <sangshilong@miapsoft.com>
#  Create_time  :  2010-03-15
#  Modify_time  :  2010-03-17
#              update : 修正共享连接问题
#  Execute      :  perl  
##########################################################################
select STDERR; $|=1;
select STDOUT; $|=1;

use DBI;
use strict;
use warnings; 


# access the module for DB2  Utility functions
use DB2Util;
 


sub getAccount{
  my $sql = "select count(0) from mk_vsdm.a";
	my @aaa = ETL::getSelect( $sql )->fetchrow();
	return $aaa[0];
}

  
	
	
sub main
{
	print "===========";
	print "main  calling";
  print getAccount();
  my $sql=" insert into mk_vsdm.a values(13);
						insert into mk_vsdm.a values(13);
						insert into mk_vsdm.a values(13);
						insert into mk_vsdm.a values(13);
						insert into mk_vsdm.a values(13);
						insert into mk_vsdm.a values(13);
	";
	ETL::run_db2cmd($sql);
	
	print getAccount();
	
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



 



