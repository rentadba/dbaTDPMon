USE [dbaTDPMon]
GO

RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *', 10, 1) WITH NOWAIT
RAISERROR('* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* Patch script: from version 2017.6 to 2017.10 (2017.10.02)				  *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2017.10.02' WHERE [module] = 'common' AND [name] = 'Application Version'
GO


/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: COMMON', 10, 1) WITH NOWAIT

RAISERROR('Create procedure: [dbo].[usp_changeServerOption_xp_cmdshell]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_changeServerOption_xp_cmdshell]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_changeServerOption_xp_cmdshell]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_changeServerOption_xp_cmdshell]
		  @serverToRun			[sysname]
		, @flgAction			[tinyint] = 1 -- 1=enable | 0=disable
		, @optionXPValue		[bit] = 0 OUTPUT
		, @debugMode			[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2017 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 02.10.2017
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE @queryToRun					[nvarchar](1024), 
		@optionXPIsAvailable		[bit],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit],
		@currentSPIDCounterValue	[int],
		@currentAllCounterValue		[int]

SET NOCOUNT ON

/*-------------------------------------------------------------------------------------------------------------------------------*/
SELECT  @optionXPIsAvailable		= 0,
		@optionXPValue				= 0,
		@optionXPHasChanged			= 0,
		@optionAdvancedIsAvailable	= 0,
		@optionAdvancedValue		= 0,
		@optionAdvancedHasChanged	= 0,
		@currentSPIDCounterValue	= 0,
		@currentAllCounterValue		= 0

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @flgAction = 1
	begin
		IF OBJECT_ID('tempdb..##tdp_xp_cmdshell_requests') IS NULL
			CREATE TABLE ##tdp_xp_cmdshell_requests
				(
					  [spid]						[smallint]	NOT NULL
					, [option_xp_changed]			[bit]		NOT NULL DEFAULT (0)
					, [option_advanced_changed]		[bit]		NOT NULL DEFAULT (0)
					, [counter]						[int]		NOT NULL DEFAULT (0)
				)

		/* try to update counter value */
		UPDATE ##tdp_xp_cmdshell_requests 
			SET [counter] = [counter] + 1
		WHERE [spid] = @@SPID

		/* current session did not requested xp_cmdshell enable, yet */
		IF @@ROWCOUNT = 0 
			begin
				/* if no other session turned the option on, will enable it */
				IF NOT EXISTS (
								SELECT *
								FROM ##tdp_xp_cmdshell_requests
								WHERE [counter] > 0
							  )
					begin
						/* enable xp_cmdshell configuration option */
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																			@configOptionName	= 'xp_cmdshell',
																			@configOptionValue	= 1,
																			@optionIsAvailable	= @optionXPIsAvailable OUT,
																			@optionCurrentValue	= @optionXPValue OUT,
																			@optionHasChanged	= @optionXPHasChanged OUT,
																			@executionLevel		= 0,
																			@debugMode			= @debugMode

						IF @optionXPIsAvailable = 0
							begin
								/* enable show advanced options configuration option */
								EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																					@configOptionName	= 'show advanced options',
																					@configOptionValue	= 1,
																					@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																					@optionCurrentValue	= @optionAdvancedValue OUT,
																					@optionHasChanged	= @optionAdvancedHasChanged OUT,
																					@executionLevel		= 0,
																					@debugMode			= @debugMode

								IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
									EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																						@configOptionName	= 'xp_cmdshell',
																						@configOptionValue	= 1,
																						@optionIsAvailable	= @optionXPIsAvailable OUT,
																						@optionCurrentValue	= @optionXPValue OUT,
																						@optionHasChanged	= @optionXPHasChanged OUT,
																						@executionLevel		= 0,
																						@debugMode			= @debugMode
							end

						IF @optionXPIsAvailable=0 OR @optionXPValue=0
							begin
								set @queryToRun='xp_cmdshell component is turned off. Cannot continue.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

								RETURN 1
							end		
						ELSE
							begin
								--mark the xp_cmdshell enable request
								INSERT	INTO ##tdp_xp_cmdshell_requests([spid], [option_xp_changed], [option_advanced_changed], [counter])
										SELECT @@SPID, @optionXPHasChanged, @optionAdvancedHasChanged, 1
							end
					end
				ELSE
					begin
						/* preserve old flags, for current session */
						SELECT   @optionXPHasChanged = MAX(CAST([option_xp_changed] AS [tinyint]))
							   , @optionAdvancedHasChanged = MAX(CAST([option_advanced_changed] AS [tinyint]))
						FROM ##tdp_xp_cmdshell_requests

						--mark the xp_cmdshell enable request
						INSERT	INTO ##tdp_xp_cmdshell_requests([spid], [option_xp_changed], [option_advanced_changed], [counter])
								SELECT @@SPID, @optionXPHasChanged, @optionAdvancedHasChanged, 1
					end
			end
		ELSE
			SET @optionXPValue = 1
	end


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @flgAction = 0 AND OBJECT_ID('tempdb..##tdp_xp_cmdshell_requests') IS NOT NULL
	begin
		/* get current session counter value */
		SELECT  @currentSPIDCounterValue  = [counter]
			  , @optionXPHasChanged		  = [option_xp_changed]
			  , @optionAdvancedHasChanged = [option_advanced_changed]
		FROM ##tdp_xp_cmdshell_requests 
		WHERE [spid] = @@SPID

		/* get all sessions counter value */
		SELECT @currentAllCounterValue  = SUM([counter])
		FROM ##tdp_xp_cmdshell_requests 

		IF @currentAllCounterValue = 1
			begin
				/* disable xp_cmdshell configuration option */
				IF @optionXPHasChanged = 1
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																		@configOptionName	= 'xp_cmdshell',
																		@configOptionValue	= 0,
																		@optionIsAvailable	= @optionXPIsAvailable OUT,
																		@optionCurrentValue	= @optionXPValue OUT,
																		@optionHasChanged	= @optionXPHasChanged OUT,
																		@executionLevel		= 0,
																		@debugMode			= @debugMode

				/* disable show advanced options configuration option */
				IF @optionAdvancedHasChanged = 1
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																			@configOptionName	= 'show advanced options',
																			@configOptionValue	= 0,
																			@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																			@optionCurrentValue	= @optionAdvancedValue OUT,
																			@optionHasChanged	= @optionAdvancedHasChanged OUT,
																			@executionLevel		= 0,
																			@debugMode			= @debugMode
			end
		ELSE
			SET @optionXPValue = 1

		/* decrement counter value. when 0, remove the entry */
		SET @currentSPIDCounterValue = @currentSPIDCounterValue - 1
		
		IF @currentSPIDCounterValue = 0
			DELETE FROM ##tdp_xp_cmdshell_requests 
			WHERE [spid] = @@SPID
		ELSE
			UPDATE ##tdp_xp_cmdshell_requests
				SET [counter] = @currentSPIDCounterValue
			WHERE [spid] = @@SPID

		IF @currentAllCounterValue = 1 AND OBJECT_ID('tempdb..##tdp_xp_cmdshell_requests') IS NOT NULL
			DROP TABLE ##tdp_xp_cmdshell_requests
	end

	RETURN 0
GO


/*---------------------------------------------------------------------------------------------------------------------*/
USE [dbaTDPMon]
GO
SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO

RAISERROR('* Done *', 10, 1) WITH NOWAIT

