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
		@sqlServerName		[sysname] = @@SERVERNAME,
		@dbName				[sysname],
		@actionName			[sysname],
		@actionType			[sysname],
		@flgActions			[smallint]	= 0,
		@flgOptions			[int]	  OUTPUT,
		@agName				[sysname] OUTPUT,
		@executionLevel		[tinyint]	= 0,
		@debugMode			[bit]		= 0
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
DECLARE @clusterName				[sysname],		
		@agInstanceRoleDesc			[sysname],
		@agSynchronizationState		[sysname],
		@agPreferredBackupReplica	[bit]

SET @agName = NULL

/* get cluster name */
SET @queryToRun = N'SELECT [cluster_name] FROM sys.dm_hadr_cluster'
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

SET @queryToRun = N'SELECT @clusterName = [cluster_name]
					FROM (' + @queryToRun + N')inq'

SET @queryParameters = N'@clusterName [sysname] OUTPUT'
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

EXEC sp_executesql @queryToRun, @queryParameters, @clusterName = @clusterName OUTPUT


/* availability group configuration */
SET @queryToRun = N'
			SELECT    ag.[name]
					, ars.[role_desc]
			FROM sys.availability_replicas ar
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.[replica_id]=ar.[replica_id] AND ars.[group_id]=ar.[group_id]
			INNER JOIN sys.availability_groups ag ON ag.[group_id]=ar.[group_id]
			INNER JOIN sys.dm_hadr_availability_replica_cluster_nodes arcn ON arcn.[group_name]=ag.[name] AND arcn.[replica_server_name]=ar.[replica_server_name]
			WHERE arcn.[replica_server_name] = ''' + @sqlServerName + N''''
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

SET @queryToRun = N'SELECT    @agName = [name]
							, @agInstanceRoleDesc = [role_desc]
					FROM (' + @queryToRun + N')inq'
SET @queryParameters = N'@agName [sysname] OUTPUT, @agInstanceRoleDesc [sysname] OUTPUT'
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

EXEC sp_executesql @queryToRun, @queryParameters, @agName = @agName OUTPUT
												, @agInstanceRoleDesc = @agInstanceRoleDesc OUTPUT
	

IF @agName IS NOT NULL AND @clusterName IS NOT NULL
	begin
		/* availability group synchronization status */
		SET @queryToRun = N'
				SELECT    hdrs.[synchronization_state_desc]
						, sys.fn_hadr_backup_is_preferred_replica(''' + @dbName + N''') AS [backup_is_preferred_replica]
				FROM sys.dm_hadr_database_replica_states hdrs
				INNER JOIN sys.availability_replicas ar ON ar.[replica_id]=hdrs.[replica_id]
				INNER JOIN sys.availability_databases_cluster adc ON adc.[group_id]=hdrs.[group_id] AND adc.[group_database_id]=hdrs.[group_database_id]
				INNER JOIN sys.dm_hadr_availability_replica_cluster_states rcs ON rcs.[replica_id]=ar.[replica_id] AND rcs.[group_id]=hdrs.[group_id]
				INNER JOIN sys.databases sd ON sd.name = adc.database_name
				WHERE	ar.[replica_server_name] = ''' + @sqlServerName + N'''
						AND adc.[database_name] = ''' + @dbName + N''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

		SET @queryToRun = N'SELECT    @agSynchronizationState = [synchronization_state_desc]
									, @agPreferredBackupReplica = [backup_is_preferred_replica]
							FROM (' + @queryToRun + N')inq'

		SET @queryParameters = N'@agSynchronizationState [sysname] OUTPUT, @agPreferredBackupReplica [bit] OUTPUT'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		EXEC sp_executesql @queryToRun, @queryParameters, @agSynchronizationState = @agSynchronizationState OUTPUT
														, @agPreferredBackupReplica = @agPreferredBackupReplica OUTPUT

		SET @agSynchronizationState = ISNULL(@agSynchronizationState, '')
		SET @agInstanceRoleDesc = ISNULL(@agInstanceRoleDesc, '')
	
		IF ISNULL(@agSynchronizationState, '')<>''
			begin
				IF UPPER(@agInstanceRoleDesc) NOT IN ('PRIMARY', 'SECONDARY')
					begin
						SET @queryToRun=N'Availability Group: Current role state [ ' + @agInstanceRoleDesc + N'] does not permit the "' + @actionName + '" operation.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + @dbName + '</affected_object>' + 
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
				/* database backup - allowed actions on a secondary replica */
				IF @actionName = 'database backup' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY'
					begin								
						/* if instance is preferred replica */
						IF @agPreferredBackupReplica = 0
							begin
								SET @queryToRun=N'Availability Group: Current instance [ ' + @sqlServerName + N'] is not a backup preferred replica for the database [' + @dbName + N'].'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @eventData='<skipaction><detail>' + 
													'<name>' + @actionName + '</name>' + 
													'<type>' + @actionType + '</type>' + 
													'<affected_object>' + @dbName + '</affected_object>' + 
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
															'<affected_object>' + @dbName + '</affected_object>' + 
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
													'<affected_object>' + @dbName + '</affected_object>' + 
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
													'<affected_object>' + @dbName + '</affected_object>' + 
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
													'<affected_object>' + @dbName + '</affected_object>' + 
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
				/* database maintenance - allowed actions on a secondary replica */
				IF @actionName = 'database maintenance' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY'
					begin								
						SET @queryToRun=N'Availability Group: Operation is not supported on a secondary replica.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + @dbName + '</affected_object>' + 
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
				/* database skrink - allowed actions on a secondary replica */
				IF @actionName = 'database shrink' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY'
					begin								
						SET @queryToRun=N'Availability Group: Operation is not supported on a secondary replica.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + @dbName + '</affected_object>' + 
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

				SET @agName = @clusterName + '_' + @agName
			end
		ELSE
			SET @agName=NULL
	end

RETURN 0
GO
