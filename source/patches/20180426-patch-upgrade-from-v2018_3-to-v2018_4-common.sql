SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.3 to 2018.4 (2018.04.26)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180426-patch-upgrade-from-v2018_3-to-v2018_4-common.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='dbo' AND CONSTRAINT_NAME='FK_jobExecutionQueue_InstanceID_catalogInstanceNames')
	ALTER TABLE [dbo].[jobExecutionQueue] DROP CONSTRAINT [FK_jobExecutionQueue_InstanceID_catalogInstanceNames]
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='dbo' AND CONSTRAINT_NAME='FK_jobExecutionHistory_InstanceID_catalogInstanceNames')
	ALTER TABLE [dbo].[jobExecutionHistory] DROP CONSTRAINT [FK_jobExecutionHistory_InstanceID_catalogInstanceNames]
GO
