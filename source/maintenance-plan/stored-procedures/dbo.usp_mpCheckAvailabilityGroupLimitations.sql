RAISERROR('Create procedure: [dbo].[usp_mpCheckAvailabilityGroupLimitations]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpCheckAvailabilityGroupLimitations]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpCheckAvailabilityGroupLimitations]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpCheckAvailabilityGroupLimitations]
		@sqlServerName		 [sysname] = @@SERVERNAME,
		@dbName				 [sysname],
		@actionName			 [sysname],
		@actionType			 [sysname],
		@flgActions			 [smallint]	= 0,
		@flgOptions			 [int]	   OUTPUT,
		@clusterName		 [sysname] OUTPUT,
		@agInstanceRoleDesc	 [sysname] OUTPUT,
		@agReadableSecondary [sysname] OUTPUT,
		@executionLevel		 [tinyint]	= 0,
		@debugMode			 [bit]		= 0
/* WITH ENCRYPTION */
AS

-----------------------------------------------------------------------------------------
SET NOCOUNT ON

DECLARE		@queryToRun  					[nvarchar](2048),
			@queryParameters				[nvarchar](512),
			@nestedExecutionLevel			[tinyint],
			@eventData						[varchar](8000)

-----------------------------------------------------------------------------------------
SET @nestedExecutionLevel = @executionLevel + 1

--------------------------------------------------------------------------------------------------
DECLARE @agName						 [sysname],		
		@agSynchronizationState		 [sysname],
		@agPreferredBackupReplica	 [bit],
		@agAutomatedBackupPreference [tinyint],
		@dbIsPartOfAG				 [bit],
		@allowDBCCOnNonReadSecondary [bit]
		
SET @agName = NULL
SET @clusterName = NULL
SET @agSynchronizationState = NULL
SET @agInstanceRoleDesc = NULL
SET @dbIsPartOfAG = 0
SET @allowDBCCOnNonReadSecondary = 0

/* get cluster name */
SET @queryToRun = N' SELECT [cluster_name], CAST([db_is_part_of_ag] AS [bit]) AS [db_is_part_of_ag] 
					 FROM (SELECT [cluster_name] FROM sys.dm_hadr_cluster) hc,
						  (SELECT CASE WHEN [group_database_id] IS NOT NULL THEN 1 ELSE 0 END AS [db_is_part_of_ag] FROM sys.databases WHERE [name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''') db'
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

SET @queryToRun = N'SELECT    @clusterName = [cluster_name]
							, @dbIsPartOfAG = [db_is_part_of_ag]
					FROM (' + @queryToRun + N')inq'

SET @queryParameters = N'@clusterName [sysname] OUTPUT, @dbIsPartOfAG [bit] OUTPUT'
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

EXEC sp_executesql @queryToRun, @queryParameters, @clusterName = @clusterName OUTPUT
												, @dbIsPartOfAG = @dbIsPartOfAG OUTPUT
IF @clusterName = '' SET @clusterName = NULL

IF @clusterName IS NOT NULL AND @dbIsPartOfAG=1
	begin
		/* availability group configuration and synchronization status */
		SET @queryToRun = N'
					SELECT    ag.[name]
							, ars.[role_desc]
							, ag.[automated_backup_preference]
							, ar.[secondary_role_allow_connections_desc]
							, hdrs.[synchronization_state_desc]
							, CASE	WHEN ars.[role_desc] = ''PRIMARY'' AND ag.[automated_backup_preference] IN (0, 2, 3) THEN 1
									WHEN ars.[role_desc] = ''SECONDARY'' AND ag.[automated_backup_preference] IN (1, 2) THEN 1
									ELSE 0
							  END [backup_is_preferred_replica]
					FROM sys.availability_replicas ar
					INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.[replica_id]=ar.[replica_id] AND ars.[group_id]=ar.[group_id]
					INNER JOIN sys.availability_groups ag ON ag.[group_id]=ar.[group_id]
					INNER JOIN sys.dm_hadr_availability_replica_cluster_nodes arcn ON arcn.[group_name]=ag.[name] AND arcn.[replica_server_name]=ar.[replica_server_name]
					INNER JOIN sys.dm_hadr_database_replica_states hdrs ON ar.[replica_id]=hdrs.[replica_id]
					INNER JOIN sys.availability_databases_cluster adc ON adc.[group_id]=hdrs.[group_id] AND adc.[group_database_id]=hdrs.[group_database_id]
					WHERE arcn.[replica_server_name] = ''' + @sqlServerName + N'''
						  AND adc.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

		SET @queryToRun = N'SELECT    @agName = [name]
									, @agInstanceRoleDesc = [role_desc]
									, @agAutomatedBackupPreference = [automated_backup_preference]
									, @agReadableSecondary = [secondary_role_allow_connections_desc]
									, @agSynchronizationState = [synchronization_state_desc]
									, @agPreferredBackupReplica = [backup_is_preferred_replica]
							FROM (' + @queryToRun + N')inq'
		SET @queryParameters = N'@agName [sysname] OUTPUT, @agInstanceRoleDesc [sysname] OUTPUT, @agAutomatedBackupPreference [tinyint] OUTPUT, @agReadableSecondary [sysname] OUTPUT, @agSynchronizationState [sysname] OUTPUT, @agPreferredBackupReplica [bit] OUTPUT'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		EXEC sp_executesql @queryToRun, @queryParameters, @agName = @agName OUTPUT
														, @agInstanceRoleDesc = @agInstanceRoleDesc OUTPUT
														, @agAutomatedBackupPreference = @agAutomatedBackupPreference OUTPUT
														, @agReadableSecondary = @agReadableSecondary OUTPUT
														, @agSynchronizationState = @agSynchronizationState OUTPUT
														, @agPreferredBackupReplica = @agPreferredBackupReplica OUTPUT

		SET @agSynchronizationState = ISNULL(@agSynchronizationState, '')
		SET @agInstanceRoleDesc = ISNULL(@agInstanceRoleDesc, '')
	end
		
IF @agName IS NOT NULL AND @clusterName IS NOT NULL AND ISNULL(@agSynchronizationState, '')<>''
	begin
		IF UPPER(@agInstanceRoleDesc) NOT IN ('PRIMARY', 'SECONDARY')
			begin
				SET @queryToRun=N'Availability Group: Current role state [ ' + @agInstanceRoleDesc + N'] does not permit the "' + @actionName + '" operation.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @eventData='<skipaction><detail>' + 
									'<name>' + @actionName + '</name>' + 
									'<type>' + @actionType + '</type>' + 
									'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
									'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
									'<reason>' + @queryToRun + '</reason>' + 
								'</detail></skipaction>'

				EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
													@dbName			= @dbName,
													@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
													@eventName		= @actionName,
													@eventMessage	= @eventData,
													@eventType		= 0 /* info */

				SET @eventData='<alert><detail>' + 
								'<severity>critical</severity>' + 
								'<instance_name>' + @sqlServerName + '</instance_name>' + 
								'<cluster_name>' + @clusterName + '</instance_name>' + 
								'<availability_group_name>' + @agName + '</instance_name>' + 
								'<action_name>' + @actionName + '</action_name>' + 
								'<action_type>' + @actionType + '</action_type>' + 
								'<message>' + @queryToRun + '</message' + 
								'<event_date_utc>' + CONVERT([varchar](24), GETUTCDATE(), 121) + '</event_date_utc>' + 
								'</detail></alert>'

				EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @sqlServerName,
																@dbName					= @dbName,
																@objectName				= NULL,
																@childObjectName		= NULL,
																@module					= 'dbo.usp_mpDatabaseBackup',
																@eventName				= 'database backup',
																@parameters				= NULL,	
																@eventMessage			= @eventData,
																@dbMailProfileName		= NULL,
																@recipientsList			= NULL,
																@eventType				= 6,	/* 6 - alert-custom */
																@additionalOption		= 0

				RETURN 1
			end

		/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
		IF UPPER(@agInstanceRoleDesc) = 'SECONDARY'
			begin
				BEGIN TRY
					SELECT	@allowDBCCOnNonReadSecondary = CASE WHEN [value]='true' THEN 1 ELSE 0 END
					FROM	[dbo].[appConfigurations]
					WHERE	[name] ='Allow DBCC operations on non-readable secondary replicas (AlwaysOn)' 
							AND [module] = 'maintenance-plan'
				END TRY
				BEGIN CATCH
					SET @allowDBCCOnNonReadSecondary = 0
				END CATCH
				SET @allowDBCCOnNonReadSecondary = ISNULL(@allowDBCCOnNonReadSecondary, 0)
			end

		/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
		/* database backup - allowed actions on a secondary replica */
		IF @actionName = 'database backup' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY'
			begin	
				IF @agReadableSecondary='NO' AND @allowDBCCOnNonReadSecondary = 0
					begin								
						SET @queryToRun=N'Availability Group: Operation is not allowed on a non-readable secondary replica.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1
					end

				/* if automated_backup_preference is 0 (primary), Backups should always occur on the primary replica */
				IF @agAutomatedBackupPreference = 0
					begin
						SET @queryToRun=N'Availability Group: Current setting for Backup Preferences do not permit backups on a seconday replica (0: Primary).'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1
					end

				/* if instance is preferred replica */
				IF @agPreferredBackupReplica = 0
					begin
						SET @queryToRun=N'Availability Group: Current instance [ ' + @sqlServerName + N'] is not a backup preferred replica for the database ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1
					end

				/* copy-only full backups are allowed */
				IF @flgActions & 1 = 1 AND @flgOptions & 4 = 0
					begin
						/* on alwayson availability groups, for secondary replicas, force copy-only backups */
						IF @flgOptions & 1024 = 1024
							begin
								SET @queryToRun='Server is part of an Availability Group as a secondary replica. Forcing copy-only full backups.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
								SET @flgOptions = @flgOptions + 4
							end
						ELSE
							begin
								SET @queryToRun=N'Availability Group: Only copy-only full backups are allowed on a secondary replica.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @eventData='<skipaction><detail>' + 
													'<name>' + @actionName + '</name>' + 
													'<type>' + @actionType + '</type>' + 
													'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
													'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
													'<reason>' + @queryToRun + '</reason>' + 
												'</detail></skipaction>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																	@dbName			= @dbName,
																	@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
																	@eventName		= @actionName,
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */

								RETURN 1
							end
					end

				/* Differential backups are not supported on secondary replicas. */
				IF @flgActions & 2 = 2
					begin
						SET @queryToRun=N'Availability Group: Differential backups are not supported on secondary replicas.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1
					end
				
				/* BACKUP LOG supports only regular log backups (the COPY_ONLY option is not supported for log backups on secondary replicas).*/
				IF @flgActions & 4 = 4 AND @flgOptions & 4 = 4
					begin
						SET @queryToRun=N'Availability Group: BACKUP LOG supports only regular log backups (the COPY_ONLY option is not supported for log backups on secondary replicas).'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1
					end

				/* To back up a secondary database, a secondary replica must be able to communicate with the primary replica and must be SYNCHRONIZED or SYNCHRONIZING. */
				IF UPPER(@agSynchronizationState) NOT IN ('SYNCHRONIZED', 'SYNCHRONIZING')
					begin
						SET @queryToRun=N'Availability Group: Current secondary replica state [ ' + @agSynchronizationState + N'] does not permit the backup operation.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1
					end
			end

		/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
		/* database backup - allowed actions on a primary replica */
		IF @actionName = 'database backup' AND UPPER(@agInstanceRoleDesc) = 'PRIMARY'
			begin	
				/* if automated_backup_preference is 1 (secondary only), backups logs must be performed on secondary */
				IF @agAutomatedBackupPreference = 1 AND @flgActions & 4 = 4 /* log */
					begin
						SET @queryToRun=N'Availability Group: Current setting for Backup Preferences do not permit LOG backups on a primary replica (1: Secondary only).'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1
					end

				/* if automated_backup_preference is 2 (prefered secondary): performing backups on the primary replica is acceptable if no secondary replica is available for backup operations */
				/* full and differential backups are allowed only on primary / restrictions apply for a secondary replica */
				IF @agAutomatedBackupPreference = 2 AND @flgActions & 4 = 4 /* log */
					begin
						/* check if there are secondary replicas available to perform the log backup */
						DECLARE @agAvailableSecondaryReplicas [smallint]

						SET @queryToRun = N'SELECT COUNT(*) AS  [count_replicas]
											FROM sys.dm_hadr_database_replica_states hdrs
											INNER JOIN sys.availability_replicas ar ON ar.[replica_id]=hdrs.[replica_id]
											INNER JOIN sys.availability_databases_cluster adc ON adc.[group_id]=hdrs.[group_id] AND adc.[group_database_id]=hdrs.[group_database_id]
											INNER JOIN sys.dm_hadr_availability_replica_cluster_states rcs ON rcs.[replica_id]=ar.[replica_id] AND rcs.[group_id]=hdrs.[group_id]
											INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.[replica_id]=ar.[replica_id] AND ars.[group_id]=ar.[group_id]
											INNER JOIN sys.databases sd ON sd.name = adc.database_name
											WHERE	adc.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
													AND hdrs.[synchronization_state_desc] IN (''SYNCHRONIZED'', ''SYNCHRONIZING'')
													AND ars.[role_desc] = ''SECONDARY'''
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						SET @queryToRun = N'SELECT @agAvailableSecondaryReplicas = [count_replicas]
											FROM (' + @queryToRun + ')z'

						SET @queryParameters = N'@agAvailableSecondaryReplicas [smallint] OUTPUT'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						EXEC sp_executesql @queryToRun, @queryParameters, @agAvailableSecondaryReplicas = @agAvailableSecondaryReplicas OUTPUT

						IF @agAvailableSecondaryReplicas > 0
							begin
								SET @queryToRun=N'Availability Group: Current setting for Backup Preferences indicate that LOG backups should be perform on a secondary (current available) replica.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @eventData='<skipaction><detail>' + 
													'<name>' + @actionName + '</name>' + 
													'<type>' + @actionType + '</type>' + 
													'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
													'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
													'<reason>' + @queryToRun + '</reason>' + 
												'</detail></skipaction>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																	@dbName			= @dbName,
																	@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
																	@eventName		= @actionName,
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */

								RETURN 1
							end
					end
			end

		/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
		/* database maintenance - allowed actions on a secondary replica */
		IF @actionName = 'database maintenance' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY'
			begin								
				SET @queryToRun=N'Availability Group: Operation is not supported on a secondary replica.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @eventData='<skipaction><detail>' + 
									'<name>' + @actionName + '</name>' + 
									'<type>' + @actionType + '</type>' + 
									'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
									'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
									'<reason>' + @queryToRun + '</reason>' + 
								'</detail></skipaction>'

				EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
													@dbName			= @dbName,
													@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
													@eventName		= @actionName,
													@eventMessage	= @eventData,
													@eventType		= 0 /* info */

				RETURN 1
			end

		/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
		/* database consistency check - allowed actions on a secondary replica */
		IF @actionName = 'database consistency check' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY' 
			begin
				SET @queryToRun = NULL
				IF     (@agReadableSecondary='NO' AND @allowDBCCOnNonReadSecondary = 0)
					OR (@agReadableSecondary='NO' AND (@flgActions & 2 = 2 OR @flgActions & 16 = 16))
					begin
						SET @queryToRun=N'Availability Group: Operation is not allowed on a non-readable secondary replica.'
					end
				IF @agReadableSecondary <> 'NO' AND @sqlServerName <> @@SERVERNAME
					begin
						SET @queryToRun=N'Availability Group: Operation is not supported on a secondary replica when running remote maintenance.'
					end
				IF 	(@flgActions & 8 = 8)
					begin
						SET @queryToRun=N'Availability Group: Operation is not supported on a secondary replica.'
					end

				IF @queryToRun IS NOT NULL
					begin								
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1
					end
			end

		/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
		/* database skrink - allowed actions on a secondary replica */
		IF @actionName = 'database shrink' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY'
			begin								
				SET @queryToRun=N'Availability Group: Operation is not supported on a secondary replica.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @eventData='<skipaction><detail>' + 
									'<name>' + @actionName + '</name>' + 
									'<type>' + @actionType + '</type>' + 
									'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
									'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
									'<reason>' + @queryToRun + '</reason>' + 
								'</detail></skipaction>'

				EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
													@dbName			= @dbName,
													@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
													@eventName		= @actionName,
													@eventMessage	= @eventData,
													@eventType		= 0 /* info */

				RETURN 1

			end
	end
ELSE
	SET @clusterName=NULL	
RETURN 0
GO
