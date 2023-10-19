------------
##### Copyright (c) 2004-2023 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
------------
- Author	 	: Andrei STEFAN
- Module	 	: dbaTDPMon (Troubleshoot Database Performance / Monitoring)
- Description	: SQL Server 2000-2022 maintenance, checks and monitoring
------------

#### Initial Release
31.01.2015
* original code written between 2004-2014, under various partial solutions, reorganized and updated
* version 2015.1 released
	
------------
#### February 2015 new features & fixes
02.02.2015
* setup default database mail profile (top 1 from msdb.dbo.sysmail_profile)
* option for different email addresses for: Alerts, Job Status and Reports (dbo.appConfigurations)
* include SQL code in alert emails when on maintenance failures

03.02.2015
* fix "ALTER INDEX failed because the following SET options have incorrect settings: 'QUOTED_IDENTIFIER'" when performing index rebuild all
* add dbo.usp_mpAlterTableTriggers procedure to maintenance-plan (enable/disable all triggers for a table)
* fix various bugs and corner case scenarious to maintenance-plan 
	
04.02.2015
* when reorganizing an index, check for allow_page_locks option. if set to off, reorganize will not be performed (The index [...] on table [..] cannot be reorganized because page level locking is disabled.)
* disable/enable foreign key constraints when rebuilding a clustered index

05.02.2015
* option to rebuild/reorganize/disable all indexes using %
* dbo.usp_mpTableDataSynchronizeInsert, to be used for tables rebuild; performs: disable triggers/foreign keys/non-clustered indexes, truncate table, records copy from another table, enable triggers/foreign keys/rebuild indexes
	
06.02.2015
* maintenance plan, change the way messages are printed to a tree level format (dbo.usp_logPrintMessage)
* remove the rebuild all option from dbo.usp_mpAlterTableIndexes, each index is rebuild/logged individually

09.02.2015
* enhance the logging mechanisms

10.02.2015
* dbo.usp_mpAlterTableRebuildHeap, to be used for heap tables rebuid

11.02.2015
* changes to install/uninstall scripts. make it run on SQL Server 2000

12.02.2015
* add heap table rebuild step to user database maintenance job

19.02.2015
* add mechanism to rebuild disabled indexes/foreign keys due to internal actions

24.02.2015
* fix various bugs and corner case scenarious to maintenance-plan 

27.02.2015
* version 2015.2 released
	
------------
#### March 2015 new features & fixes
04.03.2015
* fix various bugs and corner case scenarious to maintenance-plan
* made index maintenance plan work on remote servers

06.03.2015
* add lock_timeout when altering indexes & updating statistics
* add dbo.usp_mpDatabaseBackup, stored procedure for database and log backup, local or remote server

09.03.2015
* add jobs for full database backup and transaction log backup

10.03.2015
* add backup cleanup mechanisms ("old" del file and "new" xp_delete_file)

19.03.2015
* enhance email reporting
* add some "intelligence" in maintenance jobs steps (allow few to fail but job execution will continue)

25.03.2015
* add mechanism for logging changes / actions made (dbo.logEventMessages)

27.03.2015
* enhance backup job email reporting; add also information on backupsets created

30.03.2015
* add feature in Daily Checks collect job to collect event messages / consolidation & reporting feature

31.03.2015
* fix various bugs and corner case scenarious to maintenance-plan 
* version 2015.3 released
	
------------
#### April 2015 new features & fixes
01.04.2015
* change job maintenance steps to perform all consistency checks once a week
* when running dbcc checktable, included also system tables

03.04.2015
* add domain name information to dbo.catalogMachineNames

06.04.2015
* create stored procedure for remote change of configuration options
* enhance consistency checks: only for objects with pages allocated

07.04.2015
* fix various bugs and corner case scenarious to maintenance-plan

14.04.2015
* add support for copy_only backups in AlwaysOn Availability groups secondary replicas
* skip databases which are part of log shipping when doing default backup

15.04.2015
* fix various bugs and corner case scenarious to maintenance-plan (backup on standby, readonly databases)

22.04.2015
* included dbo.sp_SQLSMTPMail (by Clinton Herring) to be used as email system for SQL 2000

23.04.2015
* add job script to create maintenance jobs to run for linked server (agentless)

27.04.2015
* add steps to shrink system databases (truncate_only) and their log files to system maintenance job

29.04.2015
* fix various bugs and corner case scenarious to maintenance-plan (dropping an user table while running index maintenance)
* fix various bugs to health-check
* add information on rules and threshold valus in health-check report
* add stored procedure and job step for collecting errorlog messages 

30.04.2015
* add errorlog analysis in health-check report: issues detected / complete details
* version 2015.4 released
	
------------
#### May 2015 new features & fixes
04.05.2015
* fix various bugs and corner case scenarious to maintenance-plan
* fix various bugs to health-check
* add new rule detection on health-check: databases with fixed files(s) size

18.05.2015
* add default option to skip tables with less total allocated pages to be analyzed when performing index maintenance

19.05.2015
* permit indexes containing columns of type XML or and filestream to be rebuild online (https://msdn.microsoft.com/en-us/library/ms190981(v=sql.110).aspx)

25.05.2015
* skip running dbcc checkalloc when running dbcc checkdb with physical_only (http://www.sqlskills.com/blogs/paul/checkdb-from-every-angle-consistency-checking-options-for-a-vldb/)
* add extended_logical_checks option for dbcc checkdb/dbcc checktable

26.05.2015
* add scalar function to help converting a LSN to numeric format
* version 2015.5 released
	
------------
#### June 2015 new features & fixes
03.06.2015
* add parameter for backup cleanup to change retention policy from days to full database backup count

09.06.2015
* change backup count retention policy to keep full and differential backups
* change retention policy to always keep a full backup when retention is set to days/backup count

10.06.2015
* fix various bugs and corner case scenarious to maintenance-plan

19.06.2015
* version 2015.6 released

------------
#### July 2015 new features & fixes
07.07.2015
* add batch file for creating maintenance plan jobs for agentless instances
* fix various bugs to health-check
* add mechanisms for ghost records cleanup and force of this operation (sp_clean_db_free_space)
	
08.07.2015
* add health-check rule for detecting databases with Improper Page Verify option: (Page Verify not CHECKSUM) or (Page Verify is NONE)
		
13.07.2015
* add support for XML (primary/secondary) and spatial index maintenance (reorganize/rebuild)
* optimize index rebuild operation: exclude dependent indexes when rebuilding a primary index (clustered or xml primary)

14.07.2015
* enable default project code option for health-check collect stored procedures
* fix various bugs and corner case scenarious to maintenance-plan / improve index maintenance flow

15.07.2015
* enhance statistics update mechanisms: will update statistics with age less than a specified parameter but with changes percent greater than other parameter

23.07.2015
* enhance foreign key disable/enable scenarious (reduce them) for maintenance-plan
* enhance index maintenance algorithms / default options to ensure minimum execution time

24.07.2015
* fix events that may trigger alerts like: Cannot disable primary key index % on table % because the table is published for replication
* documented upper level stored procedures for maintenance-plan
* version 2015.7 released
	
------------
#### August 2015 new features & fixes
03.08.2015
* change default algorithm for index rebuild to online mode (alternative will be the "space efficient" one)

04.08.2015
* check forwarded records percentage and page density in order to decide whenever to rebuild a heap (http://sqlblog.com/blogs/tibor_karaszi/archive/2014/03/06/how-often-do-you-rebuild-your-heaps.aspx)
* check page density in order to decide whenever to reorganize/rebuild an index
* add time limit option for the optimization task in maintenance plan
* version 2015.8 released

05.08.2015
* fix a bug on backup files cleanup algorithm when running for a remote server

06.08.2015
* add upper limit for page count for rebuilding indexes (@RebuildIndexPageCountLimit) - very large tables will only be REORGANIZED
* may use to implement staggered index maintenance (http://sqlmag.com/blog/efficient-index-maintenance-using-database-mirroring)
	
07.08.2015
* auto-complete value for option "Default backup location" at install time with current instance default backup directory

14.08.2015
* add fill_factor to the index details xml schema information
* fix small bug when running backup cleanup on SQL Server 2000 instances
	
17.08.2015
* documentation reviewed / corrections made (thank you Dragos Esanu)
* add health-check rule for detecting frequently fragmented indexes / for which lowering the fill-factor may prevent fragmentation

18.08.2015
* when rebuild an index online, check the SQL Server version and reset MAXDOP to 1 (automatic check and fix for KB2969896)

19.08.2015
* create simple batches in order to automate maintenance plan when running as agent on SQL Express edition
* when running DBCC CHECKDB, DATA_PURITY option will be used only when dbi_dbccFlags <> 2 (SQL Server 2005 onwards)
* fix small bug on database backup, getting state while database was restoring
* add stored procedure to automatically lower the fill-factor / rebuild all detected frequently fragmented indexes (customizable) dbo.usp_hcChangeFillFactorForIndexesFrequentlyFragmented

20.08.2015
* fix small bugs on checking database state before performing maintenance plan
* get start date and time information for jobs currently running, in health-check module

24.08.2015
* add health-check rule for detecting long running SQL Agent jobs (default more than 3 hours)
* fix small bugs when running setup on SQL Server 2000

25.08.2015
* gather running time when collecting SQL Agent job information

27.08.2015
* fixed small bugs on health-check HTML report
* version 2015.9 released
	
------------
#### September 2015 new features & fixes
02.09.2015
* add support for SQL Server 2000 when automatically lower the fill-factor (dbo.usp_hcChangeFillFactorForIndexesFrequentlyFragmented)

04.09.2015
* fixed small bugs on health-check HTML report
* move errorlog messages hardcoded filters to table dbo.catalogHardcodedFilters
* add module column to dbo.appConfigurations and dbo.reportHTMLOptions tables
* support for collecting OS events for Application, System and Setup logs (dbo.usp_hcCollectOSEventLogs)

08.09.2015
* optimize flow when rebuilding heaps and also doing index maintenance to avoid multiple operations on indexes

21.09.2015
* fixed small bug (incorrect index type reported) on xml logging in maintenance-plan module

23.09.2015
* add support for internal parallellsm (defining and running multiple SQL Agent jobs, number limited by a configuration value)

25.09.2015
* modified health-check discovery & collect job to use the internal parallelism mechanisms

28.09.2015
* merged SQL Agent jobs "dbaTDPMon - Discovery & Health Check" and "dbaTDPMon - Generate Reports"

29.09.2015
* split OS Event messages collection into 3 jobs / machine, if internal parallelism is enabled (one per log name)

30.09.2015
* add option to skip an instance or machine name from being included in the health-check report rules
* add OS event messages information to health check report
* version 2015.10 released

------------
#### October 2015 new features & fixes
01.10.2015
* add script for creating additional indexes on msdb to help improving system maintenance execution times (http://sqlperformance.com/2015/07/sql-maintenance/msdb)

02.10.2015
* add option to read last N errorlog files (to avoid losing messages while cycling the log frequently)
* change install to use DefaultData and DefaultLog registry key for database files path, if not specified

06.10.2015
* improve performance for health-check data collect and report generation process

09.10.2015
* fix small bugs on maintenance-plan (compute statistics for system tables)
* collect mounted volumes information for systems lower than 2008R2 (thank you Tomasz Kozielski - tomasz.kozielski@atos.net)

13.10.2015
* fix small bugs on maintenance-plan (check database state when performing backup)
* add monitoring module and first custom alert - free disk/volume space on project infrastructure

14.10.2015
* change install.bat, add project code as parameter and use it to configure default project
* create schema for each individual module and move tables and views

15.10.2015
* important object renaming patches
* fix overlapping internal jobs between Health-Check and DiskSpace Monitoring & Alerting

16.10.2015
* improve performance for internal parallelism mechanisms

23.10.2015
* fix small bugs on email alerting system / html report notification
* fix small bugs on health check data collection

27.10.2015
* improve Disk Space monitoring job / add run overlap check with Health-Check job

28.10.2015
* add AlwaysOn Availability Groups backup support; secondary replicas restrictions

29.10.2015
* add skipaction event message with information on skipped database backups and reasons

30.10.2015
* enhance email notification message for failing jobs

------------
#### November 2015 new features & fixes
03.11.2015
* version 2015.11 released

09.11.2015
* fixed small bugs on monitoring mechanisms

16.11.2015
* enhance AlwaysOn Availability Groups support; secondary replicas restrictions

24.11.2015
* add custom alert & monitoring for Replication Subscription Status & Latency

------------
#### December 2015 new features & fixes
04.12.2015
* fix small bugs on maintenance-plans

29.12.2015
* fix small bugs on maintenance-plans

------------
#### January 2016 new features & fixes
12.01.2016
* add monitoring job and alert for detecting long running transactions and long uncommitted transactions

13.01.2016
* add replication monitoring alert: The subscription is not active. Subscription must have active in order to post a tracer token.

19.01.2016
* add retry step for replication latency alert: an alert will be triggered only after the retry step

20.01.2016
* enhance transaction monitoring: detect and alert on tempdb space used by a single session

------------
#### February 2016 new features & fixes
03.02.2016
* add monitoring job and alert for detecting SQL Agent failed jobs

------------
#### June 2016 new features & fixes
02.06.2016
* fix various small bugs on monitoring

08.06.2016
* updated documentation with monitoring module

11.06.2016
* fix small bugs on maintenance-plans

20.06.2016
* fix bug on replication latency monitoring / false alerts due to internal job failures

21.06.2016
* updated the documentation
* fix small bugs and made dbo.usp_mpDatabaseBackupCleanup able to run as standalone (thank you Dragos Esanu)

23.06.2016
* add new option for backup cleanup, in order to speed the process: 4096 - use xp_dirtree to identify orphan backup files to be deleted

------------
#### August 2016 new features & fixes
25.08.2016
* add parallel database maintenance jobs (backups, consistency checks, index and statistics)

29.08.2016
* fix small bugs on failed jobs monitoring
	
------------
#### September 2016 new features & fixes
05.09.2016
* parallel database maintenance jobs enhancements

26.09.2016
* fix small bugs on parallel database maintenance jobs
	
------------
#### October 2016 new features & fixes
14.10.2016
* fix small bugs on parallel database maintenance jobs

26.10.2016
* add maintenance plan internal custom weekly scheduler, in order to remove DATEPART (dw) hardcoded calls from stored procedures / jobs

29.10.2016
* add "default" schedule to all internal tasks and to new projects, when added

------------
#### November 2016 new features & fixes
12.11.2016
* add default log folder parameter in appConfigurations; all created jobs will write the logs in the value path, if set

19.11.2016
* fix SQL Server 2000 installation and maintenance plans execution

------------
#### January 2017 new features & fixes
21.01.2017
* code review / code and flow optimization

------------
#### February 2017 new features & fixes
25.02.2017
* code review & bug fixes; merge code with changes made by Razvan Puscasu

------------
#### March 2017 new features & fixes
06.03.2017
* enhance backup: when performing a differential database backup, check database header for existence of a full backup (do not rely solely on msdb.dbo.backupset)

07.03.2017
* enhance performance for health-check collecting OS Event logs
* enhance install utility: check for database default locations. if not found, ask for parameters
	
21.03.2017
* save internal job statistics; add job history table and view - [dbo].[vw_jobExecutionHistory]
* save health-check database details for later capacity planning - [health-check].[vw_statsDatabaseUsageHistory]

22.03.2017
* add procedure for purging old info (events, internal job logs and capacity planning raw data)

24.03.2017
* make install / uninstall smooth; remove warning messages

26.03.2017
* allow dbcc checks to be made on secondary replicas / AlwaysOn

------------
#### April 2017 new features & fixes
19.04.2017
* fix "change backup type check" from differential to full when running on AlwaysOn AvaulabilityGroups

28.04.2017
* add support for ignoring error code 15281: SQL Server blocked access to procedure
* fix update statistics error when index name contains brackets (reported by Razvan Puscasu)

------------
#### May 2017 new features & fixes
04.05.2017
* fix database shrink error & alert when another database is in a middle of a restore (reported by Razvan Puscasu)
* fix minor bugs on maintenance-plan module

08.05.2017
* fix bugs on maintenance-plan module when running on an AlwaysOn configuration with multiple groups (mix primary/secondary) (reported by Stefan Iancu)

09.05.2017
* add email alerting flood control: allowing maximum 50 messages (default) in a 5 minutes time-frame

16.05.2017
* update check/limitations when doing online index rebuild based on version/edition 
* starting with SQL Server 2014, when doing online index rebuild, use WAIT_AT_LOW_PRIORITY(MAX_DURATION = [..] MINUTES, ABORT_AFTER_WAIT=SELF) option

17.05.2017
* perform online table/heap rebuild using WAIT_AT_LOW_PRIORITY (SQL Server 2014 onwards)

18.05.2017
* add new parameter to dbo.usp_mpDatabaseOptimize stored procedure: @skipObjectsList - comma separated list of the objects (tables, index name or stats name) to be excluded from maintenance.
* add @recreateMode option when generating parallel maintenance SQL Server agent jobs. Old/custom job definitions may be kept
* add MaxDOP option when rebuilding heap tables, default 1 (dbo.usp_mpAlterTableRebuildHeap)

24.05.2017
* add new parameter to dbo.usp_mpJobQueueCreate stored procedure: @skipDatabasesList - comma separated list of the databases to be excluded from maintenance.
	
25.05.2017
* add MaxDOP option when performing dbcc checkdb/table, default 1 (dbo.usp_mpDatabaseConsistencyCheck) (SQL Server 2014 SP2 onwards)
* fix event message XML formatting bug, when object name constain reserved chars (dbo.vw_logEventMessages)

26.05.2017
* collect AlwaysOn Availability Groups details when gathering health-check databases info (includes data loss in seconds)
* fix health-check false reporting of outdated backups in AlwaysOn Availability Group configuration

------------
#### June 2017 new features & fixes
14.06.2017
* add new parameter to dbo.usp_mpDatabaseConsistencyCheck stored procedure: @skipObjectsList - comma separated list of the objects (tables, index name or stats name) to be excluded from maintenance.

15.06.2017
* add [maintenance-plan].[objectSkipList] table, to be used the same as @skipObjectsList or @skipDatabasesList - objects to be excluded from the maintenance / per task (reported by Razvan Puscasu)

------------
#### July 2017 new features & fixes
04.07.2017
* allow database consistency checks for non-readable secondary replicas in an AlwaysOn environment (reported by Mihail Grebencio)

06.07.2017
* change procedure parameter names to mixed case 

10.07.2017
* raise error when backup file name & path are exceeding 259 characters (reported by Mihail Grebencio)

------------
#### August 2017 new features & fixes
07.08.2017
* fix "Unable to post notification to SQLServerAgent (reason: The maximum number of pending SQLServerAgent notifications has been exceeded.  The notification will be ignored.)" (reported by Razvan Puscasu)

07.08.2017
* fix a small bug when performing orphan log backup files cleanup (reported by Mihail Grebencio)

15.08.2017
* when computing elapsed transaction time, if sys.dm_tran_active_snapshot_database_transactions.elapsed_time_seconds is null, will compute duration based on sys.dm_tran_active_transactions.transaction_begin_time (reported by Razvan Puscasu)

17.08.2017
* fix a small bug when checking for an existing full database backup and "SQL Server VSS Writer" service was running

------------
#### October 2017 new features & fixes
02.10.2017
* minimize the number of server configuration option xp_cmdshell enable/disable calls

------------
#### November 2017 new features & fixes
27.11.2017	
* threat special characters in database name, for backup operation: '!@#$%^&()-={};:`"<>.,\/[[ ]

------------
#### December 2017 new features & fixes
09.12.2017	
* threat special characters for all object names, excluding instance name: '!@#$%^&()-={};:`"<>.,\/[[ ]~ by enclose all object names using a custom quote function: [dbo].[ufn_getObjectQuoteName]

14.12.2017	
* code review: use a custom logging stored procedure: [dbo].[usp_logPrintMessage]

20.12.2017	
* tested maintenance-plan module under SQL Server 2017 on Linux; add support for Linux OS
* custom backup path creation and backup cleanup/retention policy features are disabled on Linux OS
* fix maintenance-plan log backup issue, when switching recovery model from simple to full and back
* fix bug when running dbo.usp_hcCollectDatabaseDetails for databases over 2 TB in size (reported by Razvan Puscasu)

21.12.2017
* check for missing single column histograms before running sp_createstats 'indexonly'

22.12.2017
* fix issue when a job failed and it was reported as "in progress" instead of "failed" (parallel maintenance-plan)
* full testing cycle on SQL Server 2000 up to SQL Server 2017

------------
#### January 2018 new features & fixes
12.01.2018
* fix dbo.ufn_hcGetIndexesFrequentlyFragmented function when using parallel index maintenance

14.01.2018
* add utility for update support from version 2017.6 to 2017.12

25.01.2018
* update support enhancement: auto detection for installed modules at run-time

26.01.2018
* add support for updating from version 15.12, 16.6, 16.9, 16.11 and 17.4 to 17.12

28.01.2018
* uninstall utility: add user confirmation
* health-check: fix possible bcp out issue when xp_cmdshell is not enabled
* monitoring: fix possible multiple reporting for the same uncommitted transaction
* monitoring: add long session request monitoring alert (long running SQL statements) (requested by Razvan Puscasu)

31.01.2018
* monitoring: sql_handle can be used to filter out unwanted alerts ([monitoring].[alertSkipRules])
* maintenance-plan: fix small bug when detecting existence of a full database backup
* health-check: refactoring of daily report header
	
------------
#### February 2018 new features & fixes
01.02.2018
* maintenance-plan: allow updating statistics with default sample 

13.02.2018
* monitoring: fix long waits for monitoring disk space job when only monitoring module was installed (reported by George Talaba)

15.02.2018
* modify active start date for all jobs to be utility installation day (reported by George Talaba)

20.02.2018
* maintenance plan: fix small bug when building SQL statement for rebuilding index partition
* maintenance plan: add option to rebuild only a heap table partition

21.02.2018
* maintenance plan: extend support for heap/index partition maintenance (rebuild/reorganize)

22.02.2018
* maintenance plan: add support for incremental statistics (SQL Server 2014+)

------------
#### March 2018 new features & fixes
01.03.2018
* add support for ignoring error code 1927: There are already statistics on table [...] (reported by Stefan Iancu)
* disable shrink internal jobs in parallel maintenance plan (reported by Stefan Iancu)

------------
#### April 2018 new features & fixes
05.04.2018
* merging code with a 3rd party client: dbo.catalogSolutions, dbo.catalogProjects, dbo.jobExecutionQueue, dbo.jobExecutionHistory

10.04.2018
* add function [dbo].[ufn_getProjectCode](@sqlServerName, @dbName) (merging code with a 3rd party client)
* dbo.usp_refreshMachineCatalogs will make use of db_filter property from dbo.catalogProjects table

11.04.2018
* add jobExecutionStatistics, live and history, mechanism for internal parallelism and jobs execution stats (developed by Razvan Puscasu)

13.04.2018
* added database_id in the name of the internal maintenance sql agent jobs

16.04.2018
* added new option to database transaction log backup: 8192 - use tail log backup - NORECOVERY (developed by Razvan Puscasu)
* enhance project specific scheduler for maintenance-plan internal tasks
* enhance [maintenance-plan].[vw_objectSkipList] table: add instance/database information
* add MAXDOP option for update statistics (starting with SQL Server 2017 CU3)

17.04.2018
* enhance internal job queue execution mechanism (add serial mode and option to skip the execution using SQL Agent jobs) (developed by Razvan Puscasu)

18.04.2018
* add mechanism for additional recipients on monitoring alerts

23.04.2018
* include dbo.jobExecutionStatisticsHistory in internal retention policy
* health check: do not show saved detailed information in the daily report for inactive instances

24.04.2018
* maintenance plan: fix defect when rebuilding heaps with ONLINE=ON

26.04.2018
* fix small defects related to easy of use on large deploymnets (health-check, maintenance-plan, job queueing)

27.04.2018
* fix defects on SQL Agent job handling: start/stop under high concurency

30.04.2018
* maintenance-plan: fix date/time value used for file backup when running against a remote server
* monitoring: allow SQL Agent jobs to be excluded from transaction status alerts

------------
#### May 2018 new features & fixes
01.05.2018
* maintenance-plan: fix defect when generating job queue on a a very large number of databases
* add maximum value for how many internal jobs can be started on a system (Maximum SQL Agent jobs running property)

02.05.2018
* fix defect when forcing to stop a running SQL Agent job

16.05.2018
* do not allow maintenance tasks to run against objects in "offline" filegroups (Msg 1931, Level 16, State 3, Line 7: The SQL statement cannot be executed because filegroup '...' is ofin fline.)

23.05.2018
* add MAXDOP option for update statistics on SQL Server 2016 SP2
* release of dbaTDPMon - v2018.5

30.05.2018
* replace EXEC() with EXEC sp_executesql to reduce the CPU usage for the dbaTDPMon utility

31.05.2018
* maintenance-plan: in an AlwaysOn Availability Group environment, save the database backups in ClusterName folder

------------
#### June 2018 new features & fixes
05.06.2018
* add dbo.vw_jobSchedulerDetails view with details on SQL Agent jobs scheduler definition for dbaTDPMon jobs

07.06.2018
* maintenance-plan: fix clusterName path when running backup for a databases not in an AG but on a AlwaysOn setup
* removed obsolete objects from install & update utility

11.06.2018
* monitoring: add option for Transaction Status to exclude alerts based on login_name (monitoring.alertSkipRules skip_value2 column)

13.06.2018
* health-check: fix small defects on health-check report generation for projects having multiple instances
* maintenance-plan: add priority for internal maintenance tasks jobs

13.06.2018
* implemented a workaround for internal jobs left in "in progress" state

20.06.2018
* health-check: convert local time for OS events, errorlog messages and SQL Agent jobs to UTC
* health-check: if possible, the last X hours in health-check report will be filtered on the UTC columns
* add new option for job execution: Maximum job queue execution time (hours) (0=unlimited)

28.06.2018
* send custom alert when the number of concurrent running internal jobs reached the limit set
* maintenance-plan: fix deadlock on concurrent projects defining their internal queues

------------
#### July 2018 new features & fixes
03.07.2018
* health-check: save volume/disk mount point for data/log file physical location (SQL Server 2008 onwards)
* add is_resource_intensive flag for appInternalTasks

06.07.2018
* maintenance-plan: fix deadlock on concurrent projects defining their internal queues

09.07.2018
* fix defect related to duplicate jobs entries in jobExecutionQueue table
* when refreshing projects data, add only databases not allocated to other projects

10.07.2018
* fix XML parsing error when maximum jobs limit alert is triggered

11.07.2018
* health-check: fix disk space details table formatting in the HTML report
* monitoring: auto-detect project code when sending alert emails

16.07.2018
* add new option for job execution: Maximum SQL Agent jobs running on the same physical volume (0=unlimited) 

------------
#### August 2018 new features & fixes
15.08.2018
* maintenance-plan: fix defect when triggering full database backup after an Availability Group fail-over
* maintenance-plan: fix error 976 when performing backups or consistency checks against a non-readable secondary database

18.08.2018
* maintenance-plan: add active column/flag to the objectSkipList table

20.08.2018
* health-check: enhance the Availability Group databases information collector

21.08.2018
* maintenance-plan: improve performance for [dbo].[usp_mpCheckAvailabilityGroupLimitations] stored procedure; less calls to system dmvs

28.08.2018
* maintenance-plan: mark Allocation Consistency Checks (DBCC CHECKCATALOG / DBCC CHECKALLOC) as heavy operation to lower the pressure when running in parallel

------------
#### September 2018 new features & fixes
17.09.2018
* monitoring: fix defect - analyzing replication status for an offline database

22.09.2018
* maintenance-plan: skip read-only databases from index, statistics or heap maintenance
* maintenance-plan: skip backups on database snapshots as they are not allowed

27.09.2018
* health-check: fix small issue on daily HTML report data layout
* for internal jobs, save database_name in dbo.jobExecutionHistory

------------
#### October 2018 new features & fixes
10.10.2018
* allow definition of more than 32k internal jobs in jobExecutionQueue table (smallint to int change)
17.10.2018
* reduce CPU usage for the dbo.usp_sqlAgentJobCheckStatus stored procedure
24.10.2018
* fix a small bug while getting the host domain name
27.10.2018
* health-check: use powershell to get the volume disk space as 1st option; use volume_stats as 2nd
30.10.2018
* implement caching for getting SQL Server version and edition
* health-check: add default 367 days retention for historical and log tables
* maintenance-plan: avoid a deadlock scenario when performing system databases maintenance
* maintenance-plan: reduce the number of calls to AlwaysOn / cluster related dvms
31.10.2018
* implement caching for getting database properties information
* maintenance-plan: allow index fragmentation analysis for read_only databases

------------
#### November 2018 new features & fixes
01.11.2018
* discontinued support for SQL Server 2000 (latest version supporting SQL Server 2000 is 2017.12)
* health-check: fixed small bugs when running on SQL Server 2008
* release of dbaTDPMon - v2018.11

13.11.2018
* maintenance-plan: fix a bug when updating statistics with fullscan on SQL Server 2016 onwards
* remove all SQL Server 2000 related code

27.11.2018
* add option to ignore job execution limit warning alerts
* fix issue: There is already an object named '##tdp_sql_version_requests' in the database

------------
#### December 2018 new features & fixes
10.12.2018
* monitoring: add support for replication monitoring when distributor is on a remote instance

17.12.2018
* enhance caching for getting SQL Server version and edition

26.12.2016
* maintenance-plan: do not allow database backup while shrink is in progress
* maintenance-plan: do not allow index maintenance operations while shrink is in progress
* maintenance-plan: add option to create statistics on all table columns, not only on the indexed ones

27.12.2016
* maintenance-plan: fix defect with internal job scheduler and update statistics task

------------
#### January 2019 new features & fixes
09.01.2019
* maintenance-plan: implement smart differential backup; default threshold at 50% (appConfigurations and stored procedure option)
* maintenance-plan: force update statistics when the number of changes >= sqrt(1000 * rowcount)

11.01.2019
* maintenance-plan: do not run maintenance for indexes not used in the last X days (appConfigurations option)

29.01.2019
* maintenance-plan: fix small bug on redo mechanism for internal foreign key changes
* maintenance-plan: fix bug on mechanism for detecting heavy fragmented indexes

31.01.2019
* maintenance-plan: fix small bug when generating internal jobs on systems with rotating database snapshots

------------
#### February 2019 new features & fixes
12.02.2019
* health-check: add option to the daily health check report not to filter out databases per assigned project
* health-check: have database count listed in daily report

------------
#### March 2019 new features & fixes
01.03.2019
* health-check: add more messages to be filtered out from daily HTML report, errorlog section

05.03.2019
* health-check: add failed login attempts information on daily HTML report
* maintenance-plan: add option to trigger a log backup and log shrink (truncate only) after index maintenance

11.03.2019
* maintenance-plan: remove indexes no longer in use
* maintenance-plan: add option to wait for estimated recovery time to be 0 before shrink if a database is part of AlwaysOn AG

15.03.2019
* monitoring: fix "Arithmetic overflow error converting expression to data type int" for transaction status monitoring

18.03.2019
* maintenance-plan: fix issue on smart differential backup triggering at transaction log backup

29.03.2019
* health-check: enhance dbo.usp_hcCollectOSEventLogs stored procedure (add @configEventsInLastHours parameter)

------------
#### May 2019 new features & fixes
10.05.2019
* health-check: add option to limit the reported reclaimable data and log space; default beyond 10 GB

28.05.2019
* health-check: improve performance for fetching AlwaysOn details data

31.05.2019
* add SQL Agent job_id information to dbo.jobExecutionQueue table

------------
#### June 2019 new features & fixes
03.06.2019
* optimize the calls to msdb system tables when running an internal job queue

07.06.2019
* health-check: option not to display outdated backups/dbcc for database snapshot(s)

09.06.2019
* health-check: add stored procedure for reporting database(s) and backup file(s) storage capacity

10.06.2019
* health-check: add stored procedure for reporting database(s) growth

14.06.2019
* health-check: enhancements to the daily health-check report: add Capacity Analysis category

17.06.2019
* maintenance-plan: option to allow/block maintenance operations on a non-readable AlwaysOn secondary replica
* maintenance-plan: add time limit option for the consistency tasks in maintenance plan
* health-check: collect secondary replica readable state in [health-check].[statsDatabaseAlwaysOnDetails] table
* add time limit for executing internal jobs (dbo.usp_jobQueueExecute)

18.06.2019
* integrations: stored procedure for running dbcc checkdb in a time limit mode / for all databases on all servers
* fix issue which allowed running more jobs in parallel than the @parallelJobs parameter value

19.06.2019
* integrations: optimize the dbcc checkdb queue priority to take into account the run time, max dop and previously execution times
* allow executing a job queue across multiple projects, not project by project

22.06.2019
* health-check: remove unnecessary calls to sys.dm_os_volume_stats

------------
#### July 2019 new features & fixes
12.07.2019
* code tested and ready for Azure SQL Database
* release of dbaTDPMon - v2019.7

27.09.2019
* monitoring: enhance performance for failed jobs detection (dbo.usp_monGetSQLAgentFailedJobs)

------------
#### August 2019 new features & fixes
20.08.2019
* health-check: fix issue when computing growth for databases in AG and switching nodes

------------
#### September 2019 new features & fixes
07.09.2019
* add support for Azure SQL Managed Instances
* clean update scripts: allow only update from v2019.7 onwards

19.09.2019
* maintenance-plan: fix small bugs when running backup against Azure MI

29.09.2019
* maintenance-plan: add code for data copy between database / tables (dbo.usp_mpTableDataSynchronizeInsert). Add support for on-premise to Azure data copy

------------
#### October 2019 new features & fixes
07.10.2019
* improved performance for internal log messaging by running some heavy queries only if an email must be sent
* maintenance-plan: fix issue when calling maintenance code for objects having . (dot) in their name (reported by George Talaba)

13.10.2019
* monitoring: improved performance for transaction status; include the actual SQL code in the collected data / alerts sent
* option to add/remove databases from a project using name filters

22.10.2019
* health-check: incremental read for errorlog files (read only new events)

------------
#### November 2019 new features & fixes
03.11.2019
* monitoring: fix small bug when detecting active/running sessions
* maintenance-plan: fix small bugs when displaying messages during maintenance run time
* health-check: add option to run the daily report for a single instance 
* health-check: add database(s) growth information for the last X days to the daily report

05.11.2019
* health-check: add database(s) backup size information for the last X days to the daily report

14.11.2019
* fix small performance issue on saving internal job execution statistics

29.11.2019
* maintenance-plan: do not create jobs for databases on an AlwaysOn Availability Group Non-Readable Secondary Replica setup if denied by dbo.appConfigurations

------------
#### December 2019 new features & fixes
03.12.2019
* project removal when removing the last instance assigned
* health-check: collect also internal job execution history from "monitored" instances (dbo.jobExecutionHistory)

05.12.2019
* add check for SQL Agent service before starting executing jobs in parallel mode

10.12.2019
* add code for detecting databases not managed by dbaTDPMon utility
* release of dbaTDPMon - v2019.12

14.12.2019
* maintenance-plan: stored procedure for defining default parallel maintenance jobs for a project (dbo.usp_mpJobProjectDefaultPlanCreate)
* fix update failures when running on a different database collate vs. the server one
* maintenance-plan: enhance mechanism for defining internal jobs execution order

15.12.2019
* add a non zero retry value for own SQL Agent jobs steps

16.12.2019
* maintenance-plan: fix job execution statistics view (issue reported by Handerson Dutra)

20.12.2019
* small fixes on health-check, maintenance-plan and integration modules

------------
#### January 2020 new features & fixes
14.01.2020
* release of dbaTDPMon - v2020.01
* change license type from GPL-v3 to MIT. My gift to the community. Enjoy it!

22.01.2020
* fix installation issue when placing project_code parameter between quotes (reported by Razvan Puscasu)
* health-check: fix small bug on replacing the html report file path space with underscore (reported by Razvan Puscasu)

------------
#### February 2020 new features & fixes
28.02.2020
* fix issue on checking existence / creating network folder

------------
#### June 2020 new features & fixes
17.06.2020
* reduce tempdb metadata contention (replace truncate on temp tables with delete)

19.06.2020
* fix infinit loop execution in dbo.usp_jobQueueGetStatus (reported by Razvan Puscasu)

------------
#### December 2020 new features & fixes
14.12.2020
* fix bug on processing database details, health-check module (reported by George Talaba)

------------
#### January 2021 new features & fixes
06.01.2021
* fix bug on cleansing/deleting intermediate SQL Agent jobs created for parallel execution

10.01.2021
* allow installation/objects creation into an existing database / do not force "own" database (reported by Alexey Sviridov)
* fix minor bugs related to upgrade from previous versions
* fix issues when destination database collation was different from the server one
* health-check: fix table format on backup size details report section 
* fix "Warning: Null value is eliminated by an aggregate or other SET operation." messages in the execution logs

14.01.2021
* maintenance-plan: fix backup directory creation when running under Linux

21.01.2021
* release of dbaTDPMon - v2021.01

------------
#### February 2021 new features & fixes
14.02.2021
* fix deadlock occurring when generating queues for running multiple parallel jobs
* fix issue related to replication monitoring when running in single thread mode

16.02.2021
* fix issue with "Invalid object name '##tdp_xp_cmdshell_requests'" when running in high concurrency / parallel jobs
* create a new log table "[dbo].[logInternalConfigurationChanges]" to list temporary changes to instance options, when performed

19.02.2021
* fix error 3906: Failed to update database "%" because the database is read-only"; do not allow DBCC CHECKCATALOG to be executed on secondary AG replicas 

22.02.2021
* health-check: fix small issues on information displayed within the HTML report

------------
#### January 2022 new features & fixes
31.01.2021
* health-check: add more details for AlwaysOn Availability Group collected information

------------
#### February 2022 new features & fixes
20.02.2021
* health-check: add AlwaysOn Availability Group details into the health check report
* health-check: collect database backup information taken with other tools then dbaTDPMon (enhance Backup Size details in the HTML report)
* health-check: fixed minor bugs on data collection
* release of dbaTDPMon - v2022.02

------------
#### May 2023 new features & fixes
10.05.2023
* maintenance: add support for remote maintenance of system databases

15.05.2023
* maintenance: fixed issues related to alerts when running remote maintenance against a read-only secondary replica

26.07.2023
* health-check: collect database file(s) growth events from default trace files into [health-check].[statsDatabaseGrowth]

------------
#### September 2023 new features & fixes
16.09.2023
* maintenance-plan: add support for columnstore indexes maintenance (pending on deleted records and deleted segments "fragmentation")

19.09.2023
* maintenance-plan: fix issue related to rebuilding a compressed index which led to removing the data compression option
* maintenance-plan: fix issue related to rebuilding dependent indexes for a primary key - non-clustered indexes were rebuilt twice
* maintenance-plan: fix issue related to running dbcc checktable with (data_purity) even that is was not needed (run only when dbi_dbccFlags=0)

24.09.2023
* maintenance-plan: fix issue related to online index rebuild (in some scenarios offline mode was forced)

------------
#### October 2023 new features & fixes
14.10.2023
* change (un)install and update from batch files to using a single powershell script

18.10.2023
* health-check: fixed isues when running on linux 
* release of dbaTDPMon - v2023.10
