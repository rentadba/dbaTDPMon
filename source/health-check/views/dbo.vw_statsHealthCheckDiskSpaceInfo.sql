RAISERROR('Drop view : [dbo].[vw_statsHealthCheckDiskSpaceInfo]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_statsHealthCheckDiskSpaceInfo]'))
DROP VIEW [dbo].[vw_statsHealthCheckDiskSpaceInfo]
GO
