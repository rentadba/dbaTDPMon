-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Drop table: [maintenance-plan].[statsMaintenancePlanInternals]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[maintenance-plan].[statsMaintenancePlanInternals]') AND type in (N'U'))
DROP TABLE [maintenance-plan].[statsMaintenancePlanInternals]
GO
