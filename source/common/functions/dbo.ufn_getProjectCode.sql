RAISERROR('Create function: [dbo].[ufn_getProjectCode]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[ufn_getProjectCode]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_getProjectCode]
GO

CREATE FUNCTION [dbo].[ufn_getProjectCode]
(		
		@sqlServerName	[sysname],
		@dbName			[sysname]
)
RETURNS [varchar](32)
AS
begin
	DECLARE @projectCode [varchar](32);

	/* identify Project Code by InstanceName and Database Name */
	IF @sqlServerName IS NOT NULL AND @dbName IS NOT NULL
		SELECT   @projectCode = cp.[code]
		FROM dbo.catalogInstanceNames AS cin
		INNER JOIN dbo.catalogDatabaseNames AS cdn ON cin.[id] = cdn.[instance_id]
		LEFT JOIN dbo.catalogProjects AS cp ON cp.[id] = cdn.[project_id]
		WHERE	cin.[name] = @sqlServerName
				AND cdn.[name] = @dbName 
				AND cdn.[active] = 1 

	/* if not possible, get the default ProjectCode, as configured */
	IF @projectCode IS NULL
		begin
			SELECT	@projectCode = [value]
			FROM	[dbo].[appConfigurations]
			WHERE	[name] = 'Default project code'
					AND [module] = 'common'
		end

	RETURN @projectCode;
end
GO

