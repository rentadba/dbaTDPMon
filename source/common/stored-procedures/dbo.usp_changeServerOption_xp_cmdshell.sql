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
		@currentAllCounterValue		[int]

SET NOCOUNT ON

/*-------------------------------------------------------------------------------------------------------------------------------*/
SELECT  @optionXPIsAvailable		= 0,
		@optionXPValue				= 0,
		@optionXPHasChanged			= 0,
		@optionAdvancedIsAvailable	= 0,
		@optionAdvancedValue		= 0,
		@optionAdvancedHasChanged	= 0,
		@currentAllCounterValue		= 0

/*-------------------------------------------------------------------------------------------------------------------------------*/
BEGIN TRY
	IF @flgAction = 1
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
					SET @queryToRun='xp_cmdshell component is turned off. Cannot continue.'
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					RETURN 1
				end		
			ELSE
				begin
					/* mark the xp_cmdshell enable request */
					IF @optionXPHasChanged = 1
						INSERT	INTO [dbo].[logInternalConfigurationChanges]([instance_name], [spid], [option_xp_changed], [option_advanced_changed], [counter], [event_start_date_utc])
								SELECT @serverToRun, @@SPID, @optionXPHasChanged, @optionAdvancedHasChanged, 1, GETUTCDATE()
					ELSE
						UPDATE [dbo].[logInternalConfigurationChanges]
							SET [counter] = [counter] + 1
						WHERE	[instance_name] = @serverToRun
							AND [event_end_date_utc] IS NULL
				end
		end


	/*-------------------------------------------------------------------------------------------------------------------------------*/
	IF @flgAction = 0
		begin
			/* get current session counter value */
			SELECT  TOP 1 
					@currentAllCounterValue   = [counter]
				  , @optionXPHasChanged		  = [option_xp_changed]
				  , @optionAdvancedHasChanged = [option_advanced_changed]
			FROM [dbo].[logInternalConfigurationChanges] 
			WHERE	[instance_name] = @serverToRun
				AND [event_end_date_utc] IS NULL

			IF ISNULL(@currentAllCounterValue, 0) = 1
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

					UPDATE [dbo].[logInternalConfigurationChanges] 
						SET   [event_end_date_utc] = GETUTCDATE()
							, [counter] = 0
					WHERE	[instance_name] = @serverToRun
						AND [event_end_date_utc] IS NULL
				end

			IF ISNULL(@currentAllCounterValue, 0) > 0
				begin
					UPDATE [dbo].[logInternalConfigurationChanges]
						SET [counter] = [counter] - 1
					WHERE	[instance_name] = @serverToRun
						AND [event_end_date_utc] IS NULL
				end
			ELSE
				SET @optionXPValue = 1
		end
END TRY
BEGIN CATCH
	PRINT ERROR_MESSAGE()
	SET @optionXPValue = 1
END CATCH

RETURN 0
GO
