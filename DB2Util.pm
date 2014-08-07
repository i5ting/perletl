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
#              update : 修正共享连接问题
#
#
#
# 测试脚本内容如下：
#   select STDERR; $|=1;
#   select STDOUT; $|=1;
#   
#   use DBI;
#   use strict;
#   use warnings; 
#   
#   
#   # access the module for DB2 Sample Utility functions
#   use JyyrDB2Util;
#   
#   
#   my $sql="insert into mk_vsdm.a values(13);";
#   ETL::run_db2cmd_main($sql);
#----end
#
#   使用原则 ： 
#				少建数据库连接为上（节能减排）
#				上兵伐谋，能用SQL解决的问题不用编程解决为上（能用第二种方式就用第二种方式）
#				能利用DB提供的优化器解决为上（食敌人一石粮，胜于在自己国家去1000石）
#        利而诱之，乱而取之，实而备之，强而避之，怒而挠之，卑而骄之，逸而劳之，亲耳离之（因利而制势）
#
#   perl连接DB2方式1：DBI::DBD
#      1.1组成部分
#        -getDbHandler             同DBconnect()，建立并获取数据库连接
#        -PrepareExecuteSql        预编译SQL
#        -TransRollback            出错事务回滚
#        -disconnect               与DBconnect()相反，销毁数据库连接
#      1.2执行部分
#			  -executeSQL              执行完SQL要我们手动关闭连接，用于处理多行SQL
#        -executeOnceSQL           执行完SQL会自动关闭连接。
#        -getSelect                获取结果集，然后自己写while进行处理。（内部无日志版getStatement）
# 		    -getAllCount              获得表中记录总数，参数是表名
# 		    -getCountBySQL            通过SQL获得表中记录总数，参数是SQL，如“select count(0) from mk_vsdm.a”返回的是数组的第一个，所以不要select多个字段
#        -dropTables               删除表，如果存在DBI::DBD连接，使用它，没有就用db2cmd删除。可以容错。参数是模式名+表名组成的多个字符串数组，参数可以是1个，也可以是多个
#        -isDb2TableExist          检测表名是否存在
#
#   perl连接DB2方式2：
#
#     		run_db2cmd_main           首先会调用setUserAndPasswd获取用户名密码，然后就可以执行SQL
#      
#
#
#
#   工具方法：
#        -run_cmd                  调用bat文件  
#        -mylog                    普通日志打印
#        -mySubLog                 方法前日志打印，以+号和-号拼成的
#        -myTabLog                 在mylog日志前多一个tab
#        -myStatusLog              执行状态日志打印，在myTabLog基础上，外加3个*号
#   日期    ：
#			 -
#        -createDir							 创建相对路径
#
#
#
# TODO ：支持多个connection，很简单写一个newConnection方法，在executeSQL中加connection参数。
#
# 所谓创新：须满足2个条件，贤人闲人
##########################################################################/
use strict;
use warnings; 
use DBI;
use Cwd;#获得当前文件路径的时候用到了，建processFileName


package ETL;


#声明公共变量部分
my $SUCCESS;
my $FAILURE;
my $TRUE;
my $FALSE;
#公共变量区赋值部分
$ETL::SUCCESS = 0;
$ETL::FAILURE = 1;
$ETL::TRUE = 1;
$ETL::FALSE = 0;


# file path
my $AUTO_HOME = $ENV{"AUTO_HOME"};
my $AUTO_LOG  = "${AUTO_HOME}/LOG";
my $AUTO_DATA = "${AUTO_HOME}/DATA";



#db config
my $database_name = "hebdw";
my $db_logon_file = "Logon_vgop";
my $db_host_name = "10.129.244.22";#可以是hostname，不过一般是ip，省一步
my $db_port = "50000";
my $db_protocol = "TCPIP";
my $db_user;
my $db_password;



#date variable
my $MAXDATE = $ENV{"AUTO_MAXDATE"};
if ( !defined($MAXDATE) ) {
   $MAXDATE = "3000-12-31";
}
my $MINDATE = $ENV{"AUTO_MINDATE"};
if ( !defined($MINDATE) ) {
   $MINDATE = "1990-01-01";
}
my $NULLDATE = $ENV{"AUTO_NULLDATE"};
if ( !defined($NULLDATE) ) {
    $NULLDATE = "0001-01-01";
}
my $TX_DATE = "";
my $TX_MONTH= "";

my $D_NEXT1MONTH_FIRSTDAY = "";
my $D_NEXT1MONTH_LASTDAY = "";
my $D_NEXT1MONTH_TODAY = "";
my $D_DATE_TODAY = "";
my $D_MONTH_FIRSTDAY = "";
my $D_MONTH_LASTDAY = "";
my $D_LAST1MONTH_FIRSTDAY = "";
my $D_LAST1MONTH_LASTDAY = "";
my $D_LAST1MONTH_TODAY = "";
my $D_LAST2MONTH_FIRSTDAY = "";
my $D_LAST2MONTH_LASTDAY = "";
my $D_LAST2MONTH_TODAY = "";
my $D_LAST3MONTH_FIRSTDAY = "";
my $D_LAST3MONTH_LASTDAY = "";
my $D_LAST3MONTH_TODAY = "";
my $NEXT1MONTH_FIRSTDAY = "";
my $NEXT1MONTH_LASTDAY = "";
my $NEXT1MONTH_TODAY = "";
my $NEXT1MONTH_CHAR = "";
my $DATE_TODAY = "";
my $MONTH_FIRSTDAY = "";
my $MONTH_LASTDAY="";
my $MONTH_CHAR = "";
my $LAST1MONTH_FIRSTDAY = "";
my $LAST1MONTH_LASTDAY = "";
my $LAST1MONTH_TODAY = "";
my $LAST1MONTH_CHAR = "";
my $LAST2MONTH_FIRSTDAY = "";
my $LAST2MONTH_LASTDAY = "";
my $LAST2MONTH_TODAY = "";
my $LAST2MONTH_CHAR = "";
my $LAST3MONTH_FIRSTDAY = "";
my $LAST3MONTH_LASTDAY = "";
my $LAST3MONTH_TODAY = "";
my $LAST3MONTH_CHAR = "";
my $D_DATE_TODAY_L1 = "";
my $D_DATE_TODAY_L2 = "";
my $D_DATE_TODAY_L3 = "";
my $D_DATE_TODAY_L4 = "";
my $D_DATE_TODAY_L5 = "";
my $D_DATE_TODAY_L6 = "";
my $DATE_TODAY_L1 = "";
my $DATE_TODAY_L2 = "";
my $DATE_TODAY_L3 = "";
my $DATE_TODAY_L4 = "";
my $DATE_TODAY_L5 = "";
my $DATE_TODAY_L6 = "";
my $WEEK_OF_CALENDAR = "";
my $MONTH_OF_CALENDAR = "";
my $ISSUNDAY_FLAG = "";


$ETL::D_NEXT1MONTH_FIRSTDAY="";
$ETL::D_NEXT1MONTH_LASTDAY="";
$ETL::D_NEXT1MONTH_TODAY="";
$ETL::D_DATE_TODAY="";
$ETL::D_MONTH_FIRSTDAY="";
$ETL::D_MONTH_LASTDAY="";
$ETL::D_LAST1MONTH_FIRSTDAY="";
$ETL::D_LAST1MONTH_LASTDAY="";
$ETL::D_LAST1MONTH_TODAY="";
$ETL::D_LAST2MONTH_FIRSTDAY="";
$ETL::D_LAST2MONTH_LASTDAY="";
$ETL::D_LAST2MONTH_TODAY="";
$ETL::D_LAST3MONTH_FIRSTDAY="";
$ETL::D_LAST3MONTH_LASTDAY="";
$ETL::D_LAST3MONTH_TODAY="";
$ETL::NEXT1MONTH_FIRSTDAY="";
$ETL::NEXT1MONTH_LASTDAY="";
$ETL::NEXT1MONTH_TODAY="";
$ETL::NEXT1MONTH_CHAR = "";
$ETL::DATE_TODAY="";
$ETL::MONTH_FIRSTDAY="";
$ETL::MONTH_LASTDAY="";
$ETL::MONTH_CHAR = "";
$ETL::LAST1MONTH_FIRSTDAY="";
$ETL::LAST1MONTH_LASTDAY="";
$ETL::LAST1MONTH_TODAY="";
$ETL::LAST1MONTH_CHAR = "";
$ETL::LAST2MONTH_FIRSTDAY="";
$ETL::LAST2MONTH_LASTDAY="";
$ETL::LAST2MONTH_TODAY="";
$ETL::LAST2MONTH_CHAR = "";
$ETL::LAST3MONTH_FIRSTDAY="";
$ETL::LAST3MONTH_LASTDAY="";
$ETL::LAST3MONTH_TODAY="";
$ETL::LAST3MONTH_CHAR = "";
$ETL::D_DATE_TODAY_L1="";
$ETL::D_DATE_TODAY_L2="";
$ETL::D_DATE_TODAY_L3="";
$ETL::D_DATE_TODAY_L4="";
$ETL::D_DATE_TODAY_L5="";
$ETL::D_DATE_TODAY_L6="";
$ETL::DATE_TODAY_L1="";
$ETL::DATE_TODAY_L2="";
$ETL::DATE_TODAY_L3="";
$ETL::DATE_TODAY_L4="";
$ETL::DATE_TODAY_L5="";
$ETL::DATE_TODAY_L6="";
$ETL::WEEK_OF_CALENDAR="";
$ETL::MONTH_OF_CALENDAR="";
$ETL::ISSUNDAY_FLAG = "";

 
# declare return code, statement handler, database handler and local variable
my ($rc, $sth, $dbh);



# 创建目录的根目录
#see @ processFileName
#see @ run_perl
my $CureentDir;


##########################################################################
# Description : set db username and password 
# Input       : None
# Output      : None 
###########################################################################
sub setUserAndPasswd{
	#读取db_logon_file配置文件
  open(LOGONFILE_H, "${AUTO_HOME}/etc/${db_logon_file}");
  my $LOGON_STR = <LOGONFILE_H>;
  close(LOGONFILE_H);
	
	#获得解密后的字符串
  $LOGON_STR = `${AUTO_HOME}/bin/IceCode.exe "$LOGON_STR"`;
	
  #提取解密后的用户名和密码;
  my ($logoncmd, $userpw) = split(' ',$LOGON_STR);
	
  chop($userpw);
	
	#对全局变量赋值
  ($db_user, $db_password) = split(',' , $userpw);
}

sub setConnectString{
  #difine variable
	my $connectString="";
	#get Operation System type
  my $OS  = $^O;
  $OS =~ tr [A-Z][a-z];
	
  if ( $OS eq "aix" ) 
  {
    $connectString = "dbi:DB2:$database_name";
  }
	elsif ( $OS eq "mswin32" )
	{
    #$connectString = "dbi:ODBC:$SERVER";
    #配置走dbi::DBD的连接字符串
		$connectString = "dbi:DB2:DATABASE=$database_name; HOSTNAME=$db_host_name; PORT=$db_port; PROTOCOL=$db_protocol; ";
	}#end if
	
	return $connectString;
}

#----get DB config section

##########################################################################
# Description : a delegate mehthod . Get database connection 
#               不建议对外使用。
#               同getDbHandler()方法
# Input       : None
# Output      : database connection 
##########################################################################

sub DBconnect()
{
  unless ($dbh) {
    mylog("之前没有数据库连接或之前的连接已释放，现在开始重新连接");
	  $dbh = getDbHandler() ;
	}
  return $dbh;
}


##########################################################################
# Description : Get database connection 
#               不建议对外使用。
# Input       : None
# Output      : database connection 
##########################################################################
sub getDbHandler
{
	#first : set username and password
	setUserAndPasswd();
	
	#second : according to OS type,then set suitable connection String  
	my $connectString = setConnectString();
	
  #third : connect to the database
  mylog("Connecting to '$connectString' ...");
  my $dbh = DBI->connect($connectString, $db_user, $db_password, {AutoCommit =>0, PrintError => 1, RaiseError => 0 })
            || die "【Can't connect to $connectString】";
			
			
  mylog("Connected to database.");
  
  return $dbh ;
}

#日志没办法处理。愁也

sub run_perl
{ 
	my $arg_c = @_; # number of arguments passed to the function
	my @arg_l; # arg_l holds the values to be returned to calling function

	if($arg_c > 3 || $arg_c < 1 )
	{
		die "Usage: prog_name [fileName] [file param] [file Log] \n" ;
	}   

	# declare a variable 
	#my $fileName;
	
	#打印出当前sub的日志
  mySubLog();
	
 
	
  # if this method have 2 parameter
	if($arg_c == 3)
	{
		$arg_l[0] = $_[0];
		$arg_l[1] = $_[1];
		$arg_l[2] = $_[2];
		
		processFileName($arg_l[0]." ".$arg_l[1]." ".$arg_l[2] );
		
	}
	
	
}

##########################################################################
# Description : Get database connection 
#               注意路径url中转义问题
# Input       : None
# Output      : database connection 
# Example     :
#               ETL::processFileName("testDir.pl 20100123 log\\20100123.LOG");
#						  ETL::processFileName("testDir.pl 20100123 aae\\eres\\20100123.LOG");
#
##########################################################################
# TODO : get file real path
# 相对路径
# 
sub processFileName
{

  my ($fileName)=@_;
	print $fileName."\n";
  my $CureentDir=Cwd::getcwd();
	
	my ($perlPath,$param,$logFile);
	$_ = $fileName;
		
	if(/(\S+) (\S+) (\S+)/){
	  print "3 words were: $1 - $2 - $3\n";
		
		$perlPath =  $1 ;
		$param = $2 ;
		$logFile = $3 ;
  }
 
	$_ = $logFile;
	

	my @a = split /\\/ ,$_  ;
	my $perlLogFileName= pop(@a);
	print $perlLogFileName."\n";
	my $littlePath = join "\\\\" ,@a ;
	my $batString = $fileName."\n";
	$perlLogFileName="";
	# 判断是否有准确路径，如果有切换到相应路径下，直接cd
	# 不需要判断盘符是否一样。
	$_ = $perlPath ; 
	
  if(/(C|D|E|F|G|H):/i){ 
	  #
	  #$rootDir=substr();
		print "ssssssssssssssssssssss";
		
		my @perlFilePath = split /\\/ ,$perlPath  ;

	  pop(@perlFilePath);
		$perlLogFileName = join "\\\\" , @perlFilePath;
		
    #	设置当前路径
		setCureentDir($perlLogFileName);
		
		my $CureentDirRoot = substr($CureentDir,0,2);
		my $InputFileDirRoot = substr($fileName,0,2);
		
		my $fileRealPath = "";
		
		#将字符串全部转换成小lc()/大写字母 uc()
		$CureentDirRoot = uc($CureentDirRoot);
		$InputFileDirRoot = uc($InputFileDirRoot);		 
 
		$batString = "";
		$batString = $batString.$InputFileDirRoot." \n";
	  $batString = $batString."cd $perlLogFileName\n";
		
 	}
	
	#创建日志目录的相对路径。
	createDir($littlePath);

	#print $batString;
	my $perlcmd = $batString."db2cmd perl $perlPath  $param  >  $logFile   \nexit\n";
	
	print $perlcmd;
	
	return run_cmd($perlcmd)

}

##########################################################################
# Description : 对全局变量CureentDir进行赋值。
# Input       : 防转义的文件路径，“\\”
# Output      : None
##########################################################################
sub setCureentDir{
  ($CureentDir) = @_;
}

##########################################################################
# Description : 创建相对路径，可以同时建多个文件夹
#               注意路径url中转义问题
# Input       : None
# Output      : database connection 
# Example     :
#              ETL::createDir("aaa1\\bb\\ddddd");
#
# TODO : 如果存在是否删除
#
##########################################################################
sub createDir
{

  my ($str) = @_ ;
	
	my $path = "";

	
	unless($CureentDir){
	  $CureentDir = Cwd::getcwd();
	}
	print "CureentDir = ".$CureentDir."\n" ;
	

	# while(@a>0){
		# $path = $path.shift(@a);
		# #mkdir $path ,0755 or warn "Cannot make $path,already exist!"; 
	
	# }
	#print $#a;
	my @a = split /\\/ ,$str  ;
	
		
	my $i;
	for $i(0..$#a){
	
		if($i==0){
		
			$path = $CureentDir."/".$a[$i];
		
		}else{

			$path = $CureentDir."/".$path."/".$a[$i];
			
		}# end if
	
		mylog("now create folder :$CureentDir \ ".$path.".");
		
		mkdir $path ,0755 or warn "Cannot make $path,already exist!"; 

	}# end for
	
}
##########################################################################
# Description : Get database connection 
#               不建议对外使用。
# Input       : None
# Output      : database connection 
# Example     : 
#              注意脚本当前目录，如果当前脚本和目标地址在一个盘下，要注意位置切换
#              日志位置需要手动指定,默认为脚本当前位置
#               my $bat=" E:
#  					            cd aaa 
#  					            dir>20090308.log 或 dir>D:\20090308.log 
#  					            call test.bat 
#                      ";
#               ETL::run_cmd($bat);
#
#
#
##########################################################################
sub run_cmd
{ 
	#打印出当前sub的日志
  mySubLog();
	
  my ($cmd)=@_;
	
	my $rc = open(DB2CMD, "|cmd"); 
	unless ($rc) {
		mylog("Could not invoke CMD command");
		return -1;
	}
	#print $rc;
	#print DB2CMD <<ENDINOUT;
  print DB2CMD <<ENDINOUT;
  $cmd
ENDINOUT
  close(DB2CMD);
  my $RET_CODE = $? >> 8 ;
  print "RET_CODE,$RET_CODE";
	
  # if the return code is 12, that means something error happen
  # so we return 1, otherwise, we return 0 means ok
	
  if ( $RET_CODE == 0 or $RET_CODE == 1 or $RET_CODE == 2 ){
    showTime();
		myStatusLog("execute bat sucess");
    return $SUCCESS;  
  }
  else 
  {
    showTime();
		myStatusLog("execute bat fail");
    return $FAILURE;
   }

}

##########################################################################
# Description : execute DB2 SQL command .this is static SQL
#               actually,we use db's command to execute SQL.
# Input       : sql
# Output      : the executed return code.
# Example     :
#							my $sql="insert into mk_vsdm.a values(13);";
#							ETL::run_db2cmd($sql);
# TODO        : 虽说此方法应该有返回值，但是不知道为什么没有返回来？待改。
###########################################################################
sub run_db2cmd
{
	#打印出当前sub的日志
  mySubLog();
	
  my ($sql)=@_;
	
  #first : set username and password
	setUserAndPasswd();
	
	#把sql中的分号（‘;’）干掉
	#$sql=~ s/;//g;
	
	#print "execute run_db2cmd()\n";
  my $rc = open(DB2CMD, "| db2 -mtvsp-");  #执行失败不可继续建表或视图
  # To see if DB2CMD command invoke ok?
  unless ($rc) {
    print "Could not invoke DB2CMD command/n";
		return -1;
	}


print DB2CMD <<ENDINOUT;
----------------------------------------------------------
----------------------------------------------------------
CONNECT TO HEBdw USER $db_user USING $db_password;
-----------------------------------------------------------
-----------------------------------------------------------
------------------SQL语句开始-----------------------------
-----------------------------------------------------------
 $sql
-------------------------------------------------------
-------------------------------------------------------
CONNECT RESET;
TERMINATE;
------------------------------------------------------
----------------SQL语句结束 -------------------------
------------------------------------------------------
ENDINOUT

   close(DB2CMD);
 
	 
	my $RET_CODE = $? >> 8 ;
  if ( $RET_CODE == 0 or $RET_CODE == 1 ) 
  {
		showTime();
		myStatusLog("execute sucess");
    return $SUCCESS;
  }
  elsif ( $RET_CODE == 2 )
  {
    showTime();
		myStatusLog("execute sucess");
    print  "加载文件 $sql 告警:$RET_CODE \n"; 
    return $SUCCESS;  
  }
  else 
  {
    showTime();
    print  "加载文件 $sql 发生错误! 错误代码:$RET_CODE \n"; 
    return $FAILURE;
  }
};


#检测表名是否存在
#在dropTables()方法里有调用。
sub isDb2TableExist
{
  my ($tab_name)=@_;
	
	$tab_name = uc($tab_name);
	
	# mylog($tab_name);
		
	my @schema_and_table = split /\./ , $tab_name ;
	
	my $sql = "select  count(0)  from syscat.tables where tabName='".
								$schema_and_table[1]."' and tabschema='".$schema_and_table[0]."';";
								
	#mylog($sql);
	
	my $account = getCountBySQL($sql);
	
	if($account > 0){
	
	  myStatusLog($tab_name."存在");
		
		#返回值为1时，代表存在
	  return 1;
		
	}else{
	
		myStatusLog($tab_name."不存在");
		
		#返回值为0时，代表不存在
		return 0;
		
	} # end if
	 
	
}


##########################################################################
# Description : execute DB2 SQL command .if DBI::DBD connection isn't exist
#               it will Automate call DB2CMD to drop table 
#               可以容错，如果表名不存在，不会报异常或中断
# Input       : sql
# Output      : the executed return code.
# Example     :
#							my $sql="insert into mk_vsdm.a values(13);";
#							ETL::run_db2cmd($sql);
# 
###########################################################################
sub dropTables
{
	#打印出当前sub的日志
  mySubLog();
	
  my (@tab_names)=@_;
	
	
	my $i;
	my $sql;
	my $isExist = 0;
	
	for $i(0..$#tab_names){
	
		#判断表是否存在
	  $isExist = isDb2TableExist($tab_names[$i]);
		
		#如果表存在，则拼接SQL，准备删除
		if($isExist == 1){
		
			if($i==0){
		
				$sql = "drop table ".$tab_names[$i].";\n";
			
			}else{

				$sql = $sql."drop table ".$tab_names[$i].";\n";
				
			}# end if
		
		}else{
		  
			  # mylog("---".$tab_names[$i]."不存在，跳过 ---\n\n");
				
		}
		

	}# end for
	
  mylog(showTime()."--- run_db2_drop()  execute  started ---\n\n");
	# mylog("drop sql is : $sql .");
	
	unless ($dbh) {
    mylog("there is no DBI::DBD connection，we use DB2CMD execute sql to DROP tables");
	  return run_db2cmd($sql);
	}
	
	#如果有DBI::DBD connection，这样就直接用DBI::DBD来执行SQL。性能有所提升
	for $i(0..$#tab_names){
	
		$sql = "";
		$isExist  = "";
		
		#判断表是否存在
		$isExist = isDb2TableExist($tab_names[$i]);
		
			#如果表存在，则拼接SQL，准备删除
		if($isExist == 1){
		
			$sql = "drop table ".$tab_names[$i].";";
			
			executeSQL($sql);
			
		}else{
			
			mylog("---".$tab_names[$i]."不存在，跳过 ---\n\n");
			
		}

	}# end for
 
  # mylog("run_db2_drop()  execute  finished .");
	mylog("\n\n\n--- run_db2_drop()  execute  finished ---");
	
};

##########################################################################
# Description : Checks and parses the command line arguments
# Input       : An array containing the command line arguments that was 
#               passed to the calling function
# Output      : Database name, user name and password 
###########################################################################
sub CmdLineArgChk
{
my $arg_c = @_; # number of arguments passed to the function
my @arg_l; # arg_l holds the values to be returned to calling function

if($arg_c > 3 || $arg_c == 1 && ( ( $_[0] eq "?" ) ||
                                  ( $_[0] eq "-?" ) ||
                                  ( $_[0] eq "/?" ) ||
                                  ( $_[0] eq "-h" ) ||
                                  ( $_[0] eq "/h" ) ||
                                  ( $_[0] eq "-help" ) ||
                                  ( $_[0] eq "/help" ) ) )
{
  die "Usage: prog_name [dbAlias] [userId passwd] \n" ;
}   

# Use all defaults
if($arg_c == 0)
{
  $arg_l[0] = "dbi:DB2:sample";
  $arg_l[1] = "";
  $arg_l[2] = "";
}

# dbAlias specified
if($arg_c == 1)
{
  $arg_l[0] = "dbi:DB2:".$_[0];
  $arg_l[1] = "";
  $arg_l[2] = "";
}

# userId & passwd specified
if($arg_c == 2)
{
  $arg_l[0] = "dbi:DB2:sample";
  $arg_l[1] = $_[0];
  $arg_l[2] = $_[1];
}

# dbAlias, userId & passwd specified
if($arg_c == 3)
{
  $arg_l[0] = "dbi:DB2:".$_[0];
  $arg_l[1] = $_[1];
  $arg_l[2] = $_[2];
}

return @arg_l;
} # CmdLineArgChk

##########################################################################
# Description : Prepares and Exectes the SQL statement
# Input       : Datbase handler, SQL statement 
# Output      : Statement Handler.
##########################################################################
sub PrepareExecuteSql
{
  # get the database handler and sql into local variables
  my ($dbh_loc, $sql_loc) = @_;
  
  # declare return code and statement handle
  my ($rc, $sth); 
  
  # prepare the SQL statement or call TransRollback() if it fails
  $sth = $dbh_loc->prepare($sql_loc)
    || &TransRollback($dbh_loc);

  # execute the prepared SQL statement or call TransRollback() if it fails
  $rc = $sth->execute()
    || &TransRollback($dbh_loc); 

  return $sth;   # return the statement handler
} # PrepareExecuteSql

##########################################################################
# Description : Rollback the transaction and reset the database connection
# Input       : Database handler
# Output      : None
##########################################################################
sub TransRollback
{
  # get the database handler into local variables
  my ($dbh_loc) = @_;
  
  # declare return code, statement handler and local variables
  my ($rc, $sth, $no_handles, $i, $handle);

  # rollback the transaction
  mylog("Rolling back the transaction...");

  $rc = $dbh_loc->rollback()
    || die "The transaction couldn't be rolled back: $DBI::errstr";

  mylog("The transaction was rolled back.");
 
  # get the number of active statement handles currently used 
  $no_handles = $dbh_loc->{ActiveKids};

  # close all the active statement handles
  for ($i = 0; $i < $no_handles; $i++)
  {
     if($i == 0)
     {
       # no more data to be fetched from the first statement handle
       $sth->finish;
     }
     else
     {
       $handle = "\$sth$i";  # to get the subsequent statement handles
       eval "$handle->finish";
     }
  }

  # reset the connection
  mylog("Disconnecting from the database...");

  $rc = $dbh_loc->disconnect()
    || die "Disconnecting from the database failed: $DBI::errstr";

  mylog("Disconnected from the database.");

  die "\nExiting the program \n";
} # TransRollback

##########################################################################
# Description : Get database connection，and execute the SQL statement
#              not finish the statement handler，
#              when we use this method，should call  disconnect();
# Input       : sql statement
# Output      : None
# Example     :
#							ETL::executeSQL("insert into mk_vsdm.a values(13);");
#							ETL::executeSQL("insert into mk_vsdm.a values(14);");
#							ETL::executeSQL("insert into mk_vsdm.a values(15);");
#							ETL::disconnect();
##########################################################################
sub executeSQL
{
  #打印出当前sub的日志
  mySubLog();
	
  #获取参数
  my ($sql) = @_;
  
  # connect to the database
  $dbh = DBconnect();

  # populate table company_a with data. 
  my $clearSql = qq($sql);
  
  # call PrepareExecuteSql subroutine defined in JyyrDB2Util.pm
  $sth = PrepareExecuteSql($dbh, $clearSql);
  
  mylog("\n$dbh \n  now execute sql is:\n\t\" $clearSql.\"");
	
  #提交事务
  $dbh->commit() || 
          TransRollback($dbh);
					
	myStatusLog("execute sucess");

  #$rc = $sth->finish;
 
  return 0;
}


##########################################################################
# Description : Get database connection，and execute the SQL statement
#               then distinct the database connection
# Input       : sql statement
# Output      : None
# Example     : 适用于只执行一次的情况。当然可以executeSQL，后调executeOnceSQL，
#               这时就不用disconnect();
#               当调用多次的时候，不建议使用executeOnceSQL，建议使用executeSQL
#						  ETL::executeOnceSQL("insert into mk_vsdm.a values(13);");
#						  #ETL::disconnect();
##########################################################################
sub executeOnceSQL
{
  #打印出当前sub的日志
  mySubLog();
	
  #获取参数
  my ($sql) = @_;
  
  # connect to the database
  $dbh = DBconnect();
 
  # populate table company_a with data. 
  my $clearSql = qq($sql);
  
  # call PrepareExecuteSql subroutine defined in JyyrDB2Util.pm
  $sth = PrepareExecuteSql($dbh, $clearSql);
  
  mylog("$dbh \n  now execute sql is:\n\t\" $clearSql.\"\n");
	
  #提交事务
  $dbh->commit() || 
          TransRollback($dbh);

	myStatusLog("execute sucess");
		
  # no more data to be fetched from statement handleand disconnect the database connection
	disconnect();
  return 0;
}

 
##########################################################################
# 【内部使用，不对外公开】
# Description : getStatement是无日志版的getSelect。Get database connection，
#							and execute the SQL statement
#               then distinct the database connection
# Input       : sql statement
# Output      : None
# Important   : 【必须要手动关闭连接，这点一定要注意】，关闭连接之后，statement里的内容会被清空
#                须调用ETL::disconnect()手动关闭连接
# Example     :  写此方法的主要目的是在perl中实现2维数组比较麻烦
#                故给出statement接口，然后让开发人员在编写perl脚本的时候自己写while
#                示例代码：
#					my $sth=ETL::getStatement("SELECT * FROM MK_VGOP.TB_DIM_AUTO_HAND_MEASURE
#																				WHERE AUTO_HAND_FLAG='H' AND SERV_ID='S007'");
#					my @measure_array;
#					while(@measure_array=$sth->fetchrow()){ 
#						 my $id = @measure_array[0];
#						 my $name = @measure_array[1] ;
#						 print $id."   -----   ".$name."\n"
#					}
#					ETL::disconnect();#必须要手动关闭连接，这点要注意
#						   			   
##########################################################################
sub getStatement
{

  #获取参数
  my ($sql) = @_;
  
  # connect to the database
  $dbh = DBconnect();
 
  # populate table company_a with data. 
  my $clearSql = qq($sql);
  
  # call PrepareExecuteSql subroutine defined in JyyrDB2Util.pm
  $sth = PrepareExecuteSql($dbh, $clearSql);
  
	#打印出当前sub的日志
 # mylog("$dbh \n  now execute select sql is:\n\t\" $clearSql.\"\n");
	
 
  #提交事务
  $dbh->commit() || 
          TransRollback($dbh);
		
 # myStatusLog("execute  select sucess");
	
  return  $sth ;
}
 
 

##########################################################################
# Description : Get database connection，and execute the SQL statement
#               then distinct the database connection
# Input       : sql statement
# Output      : None
# Important   : 【必须要手动关闭连接，这点一定要注意】，关闭连接之后，statement里的内容会被清空
#                须调用ETL::disconnect()手动关闭连接
# Example     :  写此方法的主要目的是在perl中实现2维数组比较麻烦
#                故给出statement接口，然后让开发人员在编写perl脚本的时候自己写while
#                示例代码：
#					my $sth=ETL::getStatement("SELECT * FROM MK_VGOP.TB_DIM_AUTO_HAND_MEASURE
#																				WHERE AUTO_HAND_FLAG='H' AND SERV_ID='S007'");
#					my @measure_array;
#					while(@measure_array=$sth->fetchrow()){ 
#						 my $id = @measure_array[0];
#						 my $name = @measure_array[1] ;
#						 print $id."   -----   ".$name."\n"
#					}
#					ETL::disconnect();#必须要手动关闭连接，这点要注意
#						   			   
##########################################################################
sub getSelect
{

  #获取参数
  my ($sql) = @_;
  
  # connect to the database
  $dbh = DBconnect();
 
  # populate table company_a with data. 
  my $clearSql = qq($sql);
  
  # call PrepareExecuteSql subroutine defined in JyyrDB2Util.pm
  $sth = PrepareExecuteSql($dbh, $clearSql);
  
	#打印出当前sub的日志
  mySubLog();
	
  mylog("$dbh \n  now execute select sql is:\n\t\" $clearSql.\"\n");
	
 
  #提交事务
  $dbh->commit() || 
          TransRollback($dbh);

	myStatusLog("execute  select sucess");
		
 
  return  $sth ;
}
 

##########################################################################
# Description : get table all record account
# Input       : table name(String)
# Output      : table record account(int)
##########################################################################
sub getAllCount{

  #获取参数
  my ($tableName) = @_;
  
	#sql语句
  my $sql = "select count(0) from $tableName";
	
	#执行并获得返回的数值
	my @selectedArray = getStatement( $sql )->fetchrow();
	
	return $selectedArray[0];
}


##########################################################################
# Description : get table all record account
# Input       : table name(String)
# Output      : table record account(int)
##########################################################################
sub getCountBySQL{

  #获取参数
  my ($sql) = @_;
  
	#执行并获得返回的数值
	my @selectedArray = getStatement( $sql )->fetchrow();
	
	return $selectedArray[0];
}


##########################################################################
# Description : Disconnect the database connection
# Input       : Database handler
# Output      : None
##########################################################################
sub disconnect{
 
  #打印出当前sub的日志
  mySubLog();

	unless (!$sth) {
		# no more data to be fetched from statement handle	  
		mylog("No more data to be fetched from statement handle...");
		# Finished from statement handle
		$rc = $sth->finish;
		
		mylog("Finished from statement handle.");
	}

  mylog("Disconnecting from database...");
	# disconnect the dbh
  $dbh->disconnect
    || die "Can't disconnect from database: $DBI::errstr";
  mylog("Disconnected from database.");

}

#----utility section

##########################################################################
# Description : this is a utility method , overwrite the print
# Input       : log String
# Output      : None
##########################################################################
sub mylog{
	#获取参数
  my ($logStr) = @_;
	print "  ".$logStr."\n";

}

##########################################################################
# Description : this is a utility method , overwrite the print with a tab
# Input       : log String
# Output      : None
##########################################################################
sub myTablog{
	#获取参数
  my ($logStr) = @_;
	print "\t".$logStr."\n";

}

##########################################################################
# Description : this is a utility method , overwrite the print with “【】”
# Input       : log String
# Output      : None
# Example     : myStatusLog("execute success");
#               show like this:【execute success】
##########################################################################
sub myStatusLog{
	#获取参数
  my ($logStr) = @_;
	myTablog( "***  ".$logStr."");

}



##########################################################################
# Description : this is a utility method , overwrite the print with 
#                a lot of Charactors ,this is a method begin
# Input       : log String
# Output      : None
# Example     : myStatusLog("execute success");
#               show like this:【execute success】
##########################################################################
sub mySubLog{
	#获取参数
  my ($logStr) = @_;
	mylog( "\n+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+-");

}
##########################################################################
# Description : this is a utility method , overwrite the return Current Time
# Input       : log String
# Output      : None
# Example     :  
#                 my $ct = ETL::getCurrentTime();
#                 
##########################################################################
sub getCurrentTime
{
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
  my $current = "";

  $hour = sprintf("%02d", $hour);
  $min  = sprintf("%02d", $min);
  $sec  = sprintf("%02d", $sec);

	# 合成字符串
  $current = "${hour}:${min}:${sec}";

  return $current;
} 
 
##########################################################################
# Description : this is a utility method , overwrite the print with “【】”
# Input       : date type(just like:year,month···) String
# Output      : None
# Example     :  
#               my $year = ETL::getTimeBy("year");
##########################################################################
sub getTimeBy
{
	my ($param)  = @_;
	
	my $joinString="";
	
	#mday 是本月的第n天
	#wday 是本周的第n天
	#yday 是本年的第n天
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
	
	if($param eq "year"){
	
		$joinString = $joinString.$year;
		
		return $joinString;
	}
	
	if($param eq "mon"){
		$joinString = $joinString.$mon;
		
		return $joinString;
	}
	
	if($param eq "day"){
	
		#mday 是本月的第n天
		$joinString = $joinString.$mday;
		
		return $joinString;
	}
	
	if($param eq "hour"){
	
		$joinString = $joinString.$hour;
		
		return $joinString;
	}
	
	if($param eq "minute"){
	
		$joinString = $joinString.$min;
		
		return $joinString;
	}
	
	if($param eq "second"){
	
		$joinString = $joinString.$sec;
		
		return $joinString;
	}
	
	if($param eq "weekOfDay"){
	
	  #wday 是本周的第n天
		$joinString = $joinString.$wday;
		
		return $joinString;
	}

	if($param eq "yearOfDay"){
	
		#yday 是本年的第n天
		$joinString = $joinString.$yday;
		
		return $joinString;
	}
	
	# as default，return current day date
  return $mday;

}


##########################################################################
# Description : this is a utility method ,print current time for log 
# Input       : None
# Output      : None
# Example     :  
#               ETL::showTime();
##########################################################################
sub showTime
{
	#mday 是本月的第n天
	#wday 是本周的第n天
	#yday 是本年的第n天
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());

  my $current = "";

  $hour = sprintf("%02d", $hour);
  $min  = sprintf("%02d", $min);
  $sec  = sprintf("%02d", $sec);

  $current = "${hour}:${min}:${sec}";

  if ( defined($current) ) {
    print  ("[$current] ");
  }
  else {
    print "[$current] ";
  }
}


#----DB config section

##########################################################################
# Description : if logon_file changed,we should call this method to reset logon_file
# Input       : new  logon_file  name
# Output      : None 
###########################################################################
sub setLogonFile{
  ($db_logon_file) = @_;
}
##########################################################################
# Description : 若非必要，否则不使用。
# Input       : db username and password
# Output      : None 
###########################################################################
sub setUserAndPasswdByHand{
  ($db_user, $db_password) = @_;
}
##########################################################################
# Description : 若非必要，否则不使用。
# Input       : db username and password
# Output      : None 
###########################################################################
sub setDatabaseName{
  ($database_name) = @_;
}
##########################################################################
# Description : 若非必要，否则不使用。
# Input       : db username and password
# Output      : None 
###########################################################################
sub setHostNameAndPort{
  ($db_host_name,$db_port) = @_;
}
##########################################################################
# Description : 若非必要，否则不使用。
# Input       : db username and password
# Output      : None 
###########################################################################
sub setProtocol{
  ($db_protocol) = @_;
}

##########################################################################
# Description : 将date类型转换为字符串类型，去除"-"，与CharToDate（）相反
# Input       : date （2010-03-15）
# Output      : date （20100315）
###########################################################################
sub DateToIntChar
{
  return substr("@_",0,4).substr("@_",5,2).substr("@_",8,2);
}

##########################################################################
# Description : 将date类型转换为字符串类型，去除"-"，与DateToIntChar（）相反
# Input       : date （20100315）
# Output      : date （2010-03-15）
###########################################################################
sub CharToDate
{
  return substr("@_",0,4)."-".substr("@_",4,2)."-".substr("@_",6,2);
}
# param example : 2010-03-15
##########################################################################
# Description : 设定当日的日期。当日这个当日是可以指定的，即标量值。
# Input       : date 2010-03-15 
# Output      : date 2010-03-15
###########################################################################
sub setD_DATE_TODAY
{
  #获取参数并赋值
	($ETL::D_DATE_TODAY) = @_;

	
	
	#类型转换
	($ETL::DATE_TODAY) =DateToIntChar(@_);
	
	return $ETL::D_DATE_TODAY;
}

sub setDATE_TODAY
{
  $_ =  @_;
	
	#容错处理
  if(/-/g){ 
	  #容错处理
	  return setD_DATE_TODAY(@_);
	}
	#获取参数并赋值
	($ETL::DATE_TODAY) = @_;
	#类型转换
	($ETL::D_DATE_TODAY) =CharToDate(@_);
	
	return $ETL::DATE_TODAY;
}

##########################################################################
# Description : 初始化日期的方法，必须先调它，在其他脚本里如想使用日期
#               如 $ETL::DATE_TODAY
#               就【必须调此方法】来给这些变量赋值。
# Input       : 当日日期2010-03-15
# Output      : None
###########################################################################
sub initDate
{
  # TODO : detect the param if it contains "-"
  setD_DATE_TODAY(CharToDate($_[0]));
	sgetDATE_TODAY_L1();
	sgetDATE_TODAY_L2();
	sgetDATE_TODAY_L3();
	sgetDATE_TODAY_L4();
	sgetDATE_TODAY_L5();
	sgetDATE_TODAY_L6();	
	sgetMONTH_FIRSTDAY();
	sgetMONTH_LASTDAY();
	sgetMONTH_CHAR();
	sgetLAST1MONTH_TODAY();
	sgetLAST1MONTH_FIRSTDAY();
	sgetLAST1MONTH_LASTDAY();
	sgetLAST1MONTH_CHAR();
	sgetLAST2MONTH_TODAY();
	sgetLAST2MONTH_FIRSTDAY();
	sgetLAST2MONTH_LASTDAY();
	sgetLAST2MONTH_CHAR();
	sgetLAST3MONTH_TODAY();
	sgetLAST3MONTH_FIRSTDAY();
	sgetLAST3MONTH_LASTDAY();
	sgetLAST3MONTH_CHAR();
	sgetNEXT1MONTH_TODAY();
	sgetNEXT1MONTH_FIRSTDAY();
	sgetNEXT1MONTH_LASTDAY();
	sgetNEXT1MONTH_CHAR();
	sgetISSUNDAY_FLAG();
	sgetWEEK_OF_CALENDAR();
	sgetMONTH_OF_CALENDAR();
	
  mylog("init Date variable sucess");
	
}

##########################################################################
# Description : 设置和获取当前日期的前一天日期
# Input       : None
# Output      : date 如 20100317
###########################################################################
sub sgetDATE_TODAY_L1
{
	#取头一天
  my $sqlText ="values(char(date('$ETL::D_DATE_TODAY')-1 days))";
  my $sth=getStatement($sqlText);
	
	#从结果集从获取值
  $ETL::D_DATE_TODAY_L1=$sth->fetchrow();
 
  #类型转换
  $ETL::DATE_TODAY_L1=DateToIntChar("$ETL::D_DATE_TODAY_L1");
	
	#print "DATE_TODAY_L1=$DATE_TODAY_L1    \n";
  #print "D_DATE_TODAY_L1=$D_DATE_TODAY_L1    \n";
	
	return $ETL::DATE_TODAY_L1;
  
}

##########################################################################
# Description : 设置和获取当前日期的前二天日期
# Input       : None
# Output      : date 如 20100317
###########################################################################
#      test code
# 		ETL::setD_DATE_TODAY('2010-03-15');
# 		print ETL::getDATE_TODAY_L2()."\n";
# 		ETL::disconnect();
sub sgetDATE_TODAY_L2
{
	#取第二天
  my $sqlText ="values(char(date('$ETL::D_DATE_TODAY')-2 days))";
  my $sth=getStatement($sqlText); 
	
	#从结果集从获取值
  $ETL::D_DATE_TODAY_L2=$sth->fetchrow();
	
	#类型转换
  $ETL::DATE_TODAY_L2=DateToIntChar("$ETL::D_DATE_TODAY_L2");
	
	return $DATE_TODAY_L2;
}

##########################################################################
# Description : 设置和获取当前日期的前3天日期
# Input       : None
# Output      : date 如 20100317
###########################################################################
sub sgetDATE_TODAY_L3
{
  #取头三天
  my $sqlText ="values(char(date('$ETL::D_DATE_TODAY')-3 days))";
  my $sth=getStatement($sqlText); 
	
	#从结果集从获取值
  $ETL::D_DATE_TODAY_L3=$sth->fetchrow();
  $ETL::DATE_TODAY_L3=DateToIntChar("$ETL::D_DATE_TODAY_L3");
  #print "DATE_TODAY_L3=$DATE_TODAY_L3    \n";
  #print "D_DATE_TODAY_L3=$D_DATE_TODAY_L3    \n";
	return $ETL::DATE_TODAY_L3;
}

##########################################################################
# Description : 设置和获取当前日期的前4天日期
# Input       : None
# Output      : date 如 20100317
###########################################################################
sub sgetDATE_TODAY_L4
{
  #取头四天
  my $sqlText ="values(char(date('$ETL::D_DATE_TODAY')-4 days))";
  my $sth=getStatement($sqlText); 
  $ETL::D_DATE_TODAY_L4=$sth->fetchrow();
  $ETL::DATE_TODAY_L4=DateToIntChar("$ETL::D_DATE_TODAY_L4");
	
  #print "DATE_TODAY_L4=$DATE_TODAY_L4    \n";
  #print "D_DATE_TODAY_L4=$D_DATE_TODAY_L4    \n";

	return $ETL::DATE_TODAY_L4;
}
# test code
# ETL::setD_DATE_TODAY('2010-03-15');
# print ETL::sgetDATE_TODAY_L5()."\n";
# ETL::disconnect();

sub sgetDATE_TODAY_L5
{
  #取头五天
  my $sqlText ="values(char(date('$ETL::D_DATE_TODAY')-5 days))";
  my $sth=getStatement($sqlText); 
  $ETL::D_DATE_TODAY_L5=$sth->fetchrow();
  $ETL::DATE_TODAY_L5=DateToIntChar("$ETL::D_DATE_TODAY_L5");
  #print "DATE_TODAY_L5=$DATE_TODAY_L5    \n";
  #print "D_DATE_TODAY_L5=$D_DATE_TODAY_L5    \n";
	return $ETL::DATE_TODAY_L5;
}

sub sgetDATE_TODAY_L6
{
  #取头六天
  my $sqlText ="values(char(date('$ETL::D_DATE_TODAY')-6 days))";
  my $sth=getStatement($sqlText); 
  $ETL::D_DATE_TODAY_L6=$sth->fetchrow();
  $ETL::DATE_TODAY_L6=DateToIntChar("$ETL::D_DATE_TODAY_L6");
	
  #print "DATE_TODAY_L6=$DATE_TODAY_L6    \n";
  #print "D_DATE_TODAY_L6=$D_DATE_TODAY_L6    \n";
	return $ETL::DATE_TODAY_L6;
}

sub sgetMONTH_FIRSTDAY
{
  #取本月的第一天
  $ETL::D_MONTH_FIRSTDAY=substr("$ETL::D_DATE_TODAY",0,8)."01";
	
  $ETL::MONTH_FIRSTDAY=DateToIntChar("$ETL::D_MONTH_FIRSTDAY");
	
  #print "D_MONTH_FIRSTDAY=$D_MONTH_FIRSTDAY    \n";
  #print "MONTH_FIRSTDAY=$MONTH_FIRSTDAY    \n";
	return $ETL::MONTH_FIRSTDAY;
}


sub sgetMONTH_LASTDAY
{
  $ETL::D_MONTH_FIRSTDAY=CharToDate(sgetMONTH_FIRSTDAY()."");
  #取本月的最后一天
  my $sqlText ="values(char(date('$ETL::D_MONTH_FIRSTDAY')+1 months -1 days))";
  my $sth=getStatement($sqlText); 
  $ETL::D_MONTH_LASTDAY=$sth->fetchrow();
  $ETL::MONTH_LASTDAY=DateToIntChar("$ETL::D_MONTH_LASTDAY");
  #print "D_MONTH_LASTDAY=$D_MONTH_LASTDAY    \n";
  #print "MONTH_LASTDAY=$MONTH_LASTDAY    \n";
	return $ETL::MONTH_LASTDAY;
}

sub sgetMONTH_CHAR
{
  #取本月的char的标识方法
  $ETL::MONTH_CHAR=substr($ETL::DATE_TODAY,0,6);
  #print "MONTH_CHAR=$ETL::MONTH_CHAR    \n";
	return $ETL::MONTH_CHAR;
}

sub sgetLAST1MONTH_TODAY
{
  #取上月的本天
  my $sqlText ="values(char(date('$ETL::D_DATE_TODAY')-1 months))";
  my $sth=getStatement($sqlText); 
  $ETL::D_LAST1MONTH_TODAY=$sth->fetchrow();

  $ETL::LAST1MONTH_TODAY=DateToIntChar("$ETL::D_LAST1MONTH_TODAY");
  #print "D_LAST1MONTH_TODAY=$D_LAST1MONTH_TODAY    \n";
	return $ETL::LAST1MONTH_TODAY;
}

sub sgetLAST1MONTH_FIRSTDAY
{
  #取上月的第一天
  $ETL::D_LAST1MONTH_FIRSTDAY=substr("$ETL::D_LAST1MONTH_TODAY",0,8)."01";
  $ETL::LAST1MONTH_FIRSTDAY=DateToIntChar("$ETL::D_LAST1MONTH_FIRSTDAY");
  #print "D_LAST1MONTH_FIRSTDAY=$D_LAST1MONTH_FIRSTDAY    \n";
	return $ETL::LAST1MONTH_FIRSTDAY;
}

sub sgetLAST1MONTH_LASTDAY
{
  sgetLAST1MONTH_FIRSTDAY();
  #取上月的最后一天
  my $sqlText ="values(char(date('$ETL::D_LAST1MONTH_FIRSTDAY')+1 months -1 days))";
  my $sth=getStatement($sqlText); 
  $ETL::D_LAST1MONTH_LASTDAY=$sth->fetchrow();
 
  $ETL::LAST1MONTH_LASTDAY=DateToIntChar("$ETL::D_LAST1MONTH_LASTDAY");
  #print "D_LAST1MONTH_LASTDAY=$D_LAST1MONTH_LASTDAY    \n";
	return $ETL::LAST1MONTH_LASTDAY;
}

sub sgetLAST1MONTH_CHAR
{
  sgetLAST1MONTH_TODAY();
  #取上月的char的标识方法
  $ETL::LAST1MONTH_CHAR=substr($ETL::LAST1MONTH_TODAY,0,6);
  #print "LAST1MONTH_CHAR=$LAST1MONTH_CHAR    \n";
	return $ETL::LAST1MONTH_CHAR;
}

sub sgetLAST2MONTH_TODAY
{
  #取上上月的本天
  my $sqlText ="values(char(date('$ETL::D_DATE_TODAY')-2 months))";
  my $sth=getStatement($sqlText); 
  $ETL::D_LAST2MONTH_TODAY=$sth->fetchrow();
  
  $ETL::LAST2MONTH_TODAY=DateToIntChar("$ETL::D_LAST2MONTH_TODAY");
  #print "D_LAST2MONTH_TODAY=$D_LAST2MONTH_TODAY    \n";
	return $ETL::LAST2MONTH_TODAY;
}

sub sgetLAST2MONTH_FIRSTDAY
{
  sgetLAST2MONTH_TODAY();
  #取上上月的第一天
  $ETL::D_LAST2MONTH_FIRSTDAY=substr("$ETL::D_LAST2MONTH_TODAY",0,8)."01";
  $ETL::LAST2MONTH_FIRSTDAY=DateToIntChar("$ETL::D_LAST2MONTH_FIRSTDAY");
	 
	return $ETL::LAST2MONTH_FIRSTDAY;
}

  #print "D_LAST2MONTH_FIRSTDAY=$D_LAST2MONTH_FIRSTDAY    \n";
sub sgetLAST2MONTH_LASTDAY
{
  sgetLAST2MONTH_FIRSTDAY();
  #取上上月的最后一天
  my $sqlText ="values(char(date('$ETL::D_LAST2MONTH_FIRSTDAY')+1 months -1 days))";
  my $sth=getStatement($sqlText); 
  $ETL::D_LAST2MONTH_LASTDAY=$sth->fetchrow();
  
  $ETL::LAST2MONTH_LASTDAY=DateToIntChar("$ETL::D_LAST2MONTH_LASTDAY");
  #print "D_LAST2MONTH_LASTDAY=$D_LAST2MONTH_LASTDAY    \n";
	return $ETL::LAST2MONTH_LASTDAY;
}

sub sgetLAST2MONTH_CHAR
{
  sgetLAST2MONTH_TODAY();
  #取上上月的char的标识方法
  $ETL::LAST2MONTH_CHAR=substr($ETL::LAST2MONTH_TODAY,0,6);
  #print "LAST2MONTH_CHAR=$LAST2MONTH_CHAR    \n";
	return $ETL::LAST2MONTH_CHAR;
}

sub sgetLAST3MONTH_TODAY
{
  #取上上上月的本天
  my $sqlText ="values(char(date('$ETL::D_DATE_TODAY')-3 months))";
  my $sth=getStatement($sqlText); 
  $ETL::D_LAST3MONTH_TODAY=$sth->fetchrow();
  
  $ETL::LAST3MONTH_TODAY=DateToIntChar("$ETL::D_LAST3MONTH_TODAY");
  #print "D_LAST3MONTH_TODAY=$D_LAST3MONTH_TODAY    \n";
	return $ETL::LAST3MONTH_TODAY;
}

sub sgetLAST3MONTH_FIRSTDAY
{
  sgetLAST3MONTH_TODAY();
  #取上上上月的第一天
  $ETL::D_LAST3MONTH_FIRSTDAY=substr("$ETL::D_LAST3MONTH_TODAY",0,8)."01";
  $ETL::LAST3MONTH_FIRSTDAY=DateToIntChar("$ETL::D_LAST3MONTH_FIRSTDAY");
  #print "D_LAST3MONTH_FIRSTDAY=$D_LAST3MONTH_FIRSTDAY    \n";
	return $ETL::LAST3MONTH_FIRSTDAY;
}

sub sgetLAST3MONTH_LASTDAY
{
  sgetLAST3MONTH_FIRSTDAY();
  #取上上上月的最后一天
  my $sqlText ="values(char(date('$ETL::D_LAST3MONTH_FIRSTDAY')+1 months -1 days))";
  my $sth=getStatement($sqlText); 
  $ETL::D_LAST3MONTH_LASTDAY=$sth->fetchrow();
 
  $ETL::LAST3MONTH_LASTDAY=DateToIntChar("$ETL::D_LAST3MONTH_LASTDAY");
  #print "D_LAST3MONTH_LASTDAY=$D_LAST3MONTH_LASTDAY    \n";
	return $ETL::LAST3MONTH_LASTDAY;
}

sub sgetLAST3MONTH_CHAR
{
  sgetLAST3MONTH_TODAY();
  #取上上上月的char的标识方法
  $ETL::LAST3MONTH_CHAR=substr($ETL::LAST3MONTH_TODAY,0,6);
  #print "LAST3MONTH_CHAR=$LAST3MONTH_CHAR    \n";
	return $ETL::LAST3MONTH_CHAR;
}

sub sgetNEXT1MONTH_TODAY
{
  #取下月的本天
  my $sqlText ="values(char(date('$ETL::D_DATE_TODAY')+1 months))";
  my $sth=getStatement($sqlText); 
	
  $ETL::D_NEXT1MONTH_TODAY=$sth->fetchrow();
     
  $ETL::NEXT1MONTH_TODAY=DateToIntChar("$ETL::D_NEXT1MONTH_TODAY");
	
  #print "D_NEXT1MONTH_TODAY=$D_NEXT1MONTH_TODAY    \n";
	return $ETL::NEXT1MONTH_TODAY;
}
sub sgetNEXT1MONTH_FIRSTDAY
{
  sgetNEXT1MONTH_TODAY();
	
  #取下月的第一天
  $ETL::D_NEXT1MONTH_FIRSTDAY=substr("$ETL::D_NEXT1MONTH_TODAY",0,8)."01";
  $ETL::NEXT1MONTH_FIRSTDAY=DateToIntChar("$ETL::D_NEXT1MONTH_FIRSTDAY");
	
  #print "D_NEXT1MONTH_FIRSTDAY=$D_NEXT1MONTH_FIRSTDAY    \n";
	return $ETL::NEXT1MONTH_FIRSTDAY;
}
sub sgetNEXT1MONTH_LASTDAY
{
  sgetNEXT1MONTH_FIRSTDAY();
	
  #取下月的最后一天
  my $sqlText ="values(char(date('$ETL::D_NEXT1MONTH_FIRSTDAY')+1 months -1 days))";
  my $sth=getStatement($sqlText); 
	
  $ETL::D_NEXT1MONTH_LASTDAY=$sth->fetchrow();
  $ETL::NEXT1MONTH_LASTDAY=DateToIntChar("$ETL::D_NEXT1MONTH_LASTDAY");
	
  #print "D_NEXT1MONTH_LASTDAY=$D_NEXT1MONTH_LASTDAY    \n";
	return $NEXT1MONTH_LASTDAY;
}
sub sgetNEXT1MONTH_CHAR
{
  sgetNEXT1MONTH_TODAY();
  #取下月的char的标识方法
  $ETL::NEXT1MONTH_CHAR=substr($ETL::NEXT1MONTH_TODAY,0,6);
  #print "NEXT1MONTH_CHAR=$NEXT1MONTH_CHAR    \n";
	return $ETL::NEXT1MONTH_CHAR;
}
# 值为0是星期日     值为7是星期六
sub sgetISSUNDAY_FLAG
{
  #取今天是否星期日
	my $sqlText ="select dayofweek(
                     date('$ETL::D_DATE_TODAY')
                   )
                   from sysibm.sysdummy1";
  my $sth=getStatement($sqlText); 
	$ETL::ISSUNDAY_FLAG=$sth->fetchrow();
 
	return $ETL::ISSUNDAY_FLAG;
}
#有问题，不能通用
sub sgetWEEK_OF_CALENDAR
{
  #取出当天在日历中的周数
	my $sqlText ="select WEEK_OF_CALENDAR FROM MK_VSDM.CALENDAR WHERE CALENDAR_DATE='{$ETL::DATE_TODAY}'";
	my $sth=getStatement($sqlText); 
	
	$ETL::WEEK_OF_CALENDAR=$sth->fetchrow();
 
	return $ETL::WEEK_OF_CALENDAR;
}
#有问题，不能通用
sub sgetMONTH_OF_CALENDAR
{
  sgetNEXT1MONTH_FIRSTDAY();
	
	#取出下月在日历中的月数
	my $sqlText ="select MONTH_OF_CALENDAR FROM MK_VSDM.CALENDAR WHERE CALENDAR_DATE='{$ETL::NEXT1MONTH_FIRSTDAY}'";
	$ETL::MONTH_OF_CALENDAR=$sth->fetchrow();
	
  return $ETL::MONTH_OF_CALENDAR;
}

###########################################################################
# Description : 删除目录下所有文件
# Input       : None
# Output      : print
###########################################################################

# sub deleteFiles{
	# #检测是否文件存在	
	
	# my (@fileNames) = @_;
	
	
	 
	
	
	# foreach my $a(@fileNames){
	
	  # if(isFileExist($a)){
			
			# my @a = split /\\/ ,$str  ;
	
		
		
			# my $i;
			# for $i(0..$#a){
			
				# if($i==0){
				
					# $path = $CureentDir."/".$a[$i];
				
				# }else{

					# $path = $CureentDir."/".$path."/".$a[$i];
					
			  # }# end if
	
		  # } 
		
	# }
	
# }

sub isFileExist{
	
	my ($filename) = @_;

	if (-e $filename){
	  return 1;
	}else{
	  return 0;
	}
	
	#return default
	return 0;
	
}



sub deleteFileByName{
	my ($file_path,$file_delete_name) = @_;
	my $ret_code;
	
	 
	  opendir DH, $file_path or die  "Cannot open $file_path: $!";
 
		
		#print
	  $ret_code = unlink "$file_path\\$file_delete_name" ;
		
		if($ret_code == 1){
			print "I deleted : $ret_code  ,success \n";
		}
  
		if($ret_code == 0){
			print "I deleted : $ret_code  ,fail \n";
		}


	closedir DH;

	return $ret_code;
	
}


sub deleteFolder{
	  
	my ($dir_to_process) = @_ ;
	
	opendir DH, $dir_to_process or die  "Cannot open $dir_to_process: $!";
	foreach my $file(readdir DH) {
		
		#print
		print $dir_to_process."\\".$file."\n";
		
		#delete this file
		deleteFileByName($dir_to_process,$file);
		
	}
	closedir DH;
	
}

#---- init section
 
1; # to always return true to the calling function
