RAISERROR('Create procedure: [dbo].[usp_mpMarkInternalAction]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpMarkInternalAction]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpMarkInternalAction]
GO

CREATE PROCEDURE [dbo].[usp_mpMarkInternalAction]
		@actionName				[sysname],
		@flgOperation			[tinyint] = 1, /*	1 - insert action 
													2 - delete action
												*/
		@server_name			[sysname] = NULL,
		@database_name			[sysname] = NULL,
		@schema_name			[sysname] = NULL,
		@object_name			[sysname] = NULL,
		@child_object_name		[sysname] = NULL
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.02.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

--insert action
IF @flgOperation = 1
	begin
		INSERT	INTO [maintenance-plan].[logInternalAction]([name], [server_name], [database_name], [schema_name], [object_name], [child_object_name])
				SELECT @actionName, @server_name, @database_name, @schema_name, @object_name, @child_object_name
	end

--delete action
IF @flgOperation = 2
	begin
		IF @database_name <> '%'
			DELETE	FROM [maintenance-plan].[logInternalAction]
			WHERE	[name] = @actionName
					AND [server_name] = @server_name
					AND ([database_name] = @database_name OR ([database_name] IS NULL AND @database_name IS NULL))
					AND ([schema_name] LIKE @schema_name OR ([schema_name] IS NULL AND @schema_name IS NULL))
					AND ([object_name] LIKE @object_name OR ([object_name] IS NULL AND @object_name IS NULL))
					AND ([child_object_name] LIKE @child_object_name OR ([child_object_name] IS NULL AND @child_object_name IS NULL))
		ELSE
			DELETE	FROM [maintenance-plan].[logInternalAction]
			WHERE	[name] = @actionName
					AND [server_name] = @server_name

	end
GO
