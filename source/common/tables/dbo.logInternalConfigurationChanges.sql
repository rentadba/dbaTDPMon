-- ============================================================================
-- Copyright (c) 2004-2021 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 15.02.2021
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--log for discovery messages
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[logInternalConfigurationChanges]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[logInternalConfigurationChanges]') AND type in (N'U'))
DROP TABLE [dbo].[logInternalConfigurationChanges]
GO

CREATE TABLE [dbo].[logInternalConfigurationChanges]
(
	  [id]							[int]	 IDENTITY (1, 1)	NOT NULL
	, [instance_name]				[sysname]	NOT NULL
	, [spid]						[smallint]	NOT NULL
	, [option_xp_changed]			[bit]		NOT NULL DEFAULT (0)
	, [option_advanced_changed]		[bit]		NOT NULL DEFAULT (0)
	, [counter]						[int]		NOT NULL DEFAULT (0)
	, [event_start_date_utc]		[datetime]	NOT NULL CONSTRAINT [DF_logInternalConfigurationChanges_event_start_date_utc] DEFAULT (GETUTCDATE())
	, [event_end_date_utc]			[datetime]	NULL
) 
GO

CREATE INDEX [IX_logInternalConfigurationChanges_InstanceID] ON [dbo].[logInternalConfigurationChanges]([instance_name], [spid], [event_end_date_utc])
GO


