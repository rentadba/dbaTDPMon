RAISERROR('Drop view : [health-check].[vw_statsSQLServerAgentJobsHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsSQLServerAgentJobsHistory]'))
DROP VIEW [health-check].[vw_statsSQLServerAgentJobsHistory]
GO
