RAISERROR('Drop view : [health-check].[vw_statsSQLServerErrorlogDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsSQLServerErrorlogDetails]'))
DROP VIEW [health-check].[vw_statsSQLServerErrorlogDetails]
GO
