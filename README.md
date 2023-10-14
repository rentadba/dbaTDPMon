**dbaTDPMon - Troubleshoot Database Performance and Monitoring**

This utility is a bespoke database / system maintenance, health-check and monitoring solution for SQL Server. It runs for versions from SQL Server 2000 until 2022 plus Azure SQL Database and Managed Instances.
Some features are only available for SQL Server 2005 and beyond.
Utility consists in plain T-SQL code. A database is needed in order to store objects used by this utility.
Task automation is performed using SQL Agent jobs, pre-scheduled.
Maintenance-plan, health-check and monitoring modules can be used as “agentless” management system.

**Why dbaTDPMon?**
* implement database maintenance best practices (including system databases)
* parallel database(s) maintenance (multiple databases at once)
* automate daily health checks / HTML reporting
* can be used to administrate multiple instances from a central point
* fully customization / various options / time limit / email alerting
* check full documentation for all details

**Custom Maintenance Plan**
* **_Backup_** 
	* use checksum (+2k5) and verify the backup file
	* retention can be set to days or backup file count
	* automatically trigger a full database backup prior taking a transaction log / differential backup, if needed
* **_Consistency Checks_**
	* can be run at database or table level
	* checks are "split" over an entire week (configurable)
* **_Index Maintenance_**
	* reorganize/rebuild decision can be based on logical fragmentation or page density
	* use "drive table" to limit the number of analyzed indexes
	* 2 algorithms available: online/offline index rebuild or disable/rebuild (managing dependencies)
	* may force ghost records cleanup
* **_Columnstore indexes Maintenance_**
	* decision based on deleted records and deleted segments
* **_Heap Tables Maintenance_**
	* rebuild decision based on extent fragmentation, page density and forwarded records
* **_Statistics Maintenance_**
	* use "drive table" to limit the number of analyzed statistics
	* support for incremental statistics (+2014)
	* update decision is made based on statistics age and changes made
* **_System Maintenance_**
	* scheduled errorlog cycle
	* purge history 
	* Always On Availability Groups “aware”
	
**_Daily Health Checks & Monitoring_**
* online/offline instances and databases health state
* report failed SQL Agent jobs / disk space issues / replication issues / long or blocked transactions, etc.
* report outdated backups and checkdb
* analyze errorlogs and OS Event logs
* collect data from multiple servers in parallel
* and many more...

Reach me on:
* danandrei.stefan@gmail.com
* https://www.linkedin.com/in/danandreistefan/
* http://www.rentadba.eu

