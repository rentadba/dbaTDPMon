if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[ufn_checkIP4Address]') and xtype in (N'FN', N'IF', N'TF'))
drop function [dbo].[ufn_checkIP4Address]
GO

SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

CREATE FUNCTION dbo.ufn_checkIP4Address
(
	@ipAddress [varchar](15)
)
RETURNS bit
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2006 
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

begin 
	DECLARE @tmpStr		[varchar](15),
			@tmpIdx		[int],
			@tmpIdxOld	[int]
	
	SET @tmpIdxOld=0
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=CHARINDEX('.', @ipAddress, @tmpIdxOld+1)
	IF @tmpIdx=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpStr=null
	SET @tmpStr=SUBSTRING(@ipAddress, @tmpIdxOld+1, @tmpIdx-@tmpIdxOld-1)
	SET @tmpIdxOld=@tmpIdx
	IF LEN(ISNULL(@tmpStr, ''))=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=null
	SET @tmpIdx=CAST(@tmpStr AS integer)
	IF (@@Error<>0) OR (@tmpIdx IS NULL)
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	IF @tmpIdx>255
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=CHARINDEX('.', @ipAddress, @tmpIdxOld+1)
	IF @tmpIdx=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpStr=null
	SET @tmpStr=SUBSTRING(@ipAddress, @tmpIdxOld+1, @tmpIdx-@tmpIdxOld-1)
	SET @tmpIdxOld=@tmpIdx
	IF LEN(ISNULL(@tmpStr, ''))=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=null
	SET @tmpIdx=CAST(@tmpStr AS integer)
	IF (@@Error<>0) OR (@tmpIdx IS NULL)
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	IF @tmpIdx>255
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=CHARINDEX('.', @ipAddress, @tmpIdxOld+1)
	IF @tmpIdx=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpStr=null
	SET @tmpStr=SUBSTRING(@ipAddress, @tmpIdxOld+1, @tmpIdx-@tmpIdxOld-1)
	SET @tmpIdxOld=@tmpIdx
	IF LEN(ISNULL(@tmpStr, ''))=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=null
	SET @tmpIdx=CAST(@tmpStr AS integer)
	IF (@@Error<>0) OR (@tmpIdx IS NULL)
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	IF @tmpIdx>255
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=CHARINDEX('.', @ipAddress, @tmpIdxOld+1)
	---------------------------------------------------------------------------------------------------------
	SET @tmpStr=null
	SET @tmpStr=SUBSTRING(@ipAddress, @tmpIdxOld+1, 255)
	SET @tmpIdxOld=@tmpIdx
	IF LEN(ISNULL(@tmpStr, ''))=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=null
	SET @tmpIdx=CAST(@tmpStr AS integer)
	IF (@@Error<>0) OR (@tmpIdx IS NULL)
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	IF @tmpIdx>255
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	RETURN 0

end



GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO



