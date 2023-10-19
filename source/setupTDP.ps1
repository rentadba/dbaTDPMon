# Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# module : {all | health-check | maintenance-plan}
# useParallelExecution : [Y(es)] | N(o)
# recreateSQLAgentJobs : Y(es) | [N(o)]
# uninstall : Y(es) | [N(o)]
param (  $instanceName = "$env:ComputerName"
       , $databaseName = "dbaTDPMon"
       , $module="all"
       , $projectName="PRODUCTION"
       , $useParallelExecution="Yes"
       , $recreateSQLAgentJobs="No"
       , $dataFilePath=""
       , $logFilePath=""
       , $sqlLoginName=""
       , $sqlLoginPassword=""
       , $queryTimeout = 1800
       , $uninstall="No"
      )

push-location
import-module sqlps -disablenamechecking
pop-location

$analysisStopWatch =  [system.diagnostics.stopwatch]::StartNew()
Clear-Host

Write-Host "#-----------------------------------------------------------------------------------#"
Write-Host "dbaTDPMon (Troubleshoot Database Performance / Monitoring)"
Write-Host "https://github.com/rentadba/dbaTDPMon under MIT licence model"
Write-Host "Copyright (c) 2004-2023 Dan Andrei STEFAN (danandrei.stefan@gmail.com)"
Write-Host "#-----------------------------------------------------------------------------------#"

Write-Host "Running on instance: $instanceName"

function Exit-Fail ($message) 
{
	Write-Host "`nERROR: $message" -ForegroundColor "Red"
	Write-Host "Result:Failed." -ForegroundColor "Red"
	exit 0x1
}

function NoExit-Fail ($message) 
{
	Write-Output "`nERROR: $message" -ForegroundColor "Red"
	Write-Output "Result:Continue." -ForegroundColor "Continue"
}


Function formatElapsedTime($ts) 
{
    $elapsedTime = "-> "
    $elapsedTime += [string]::Format( "{0:00}:{1:00}:{2:00}.{3:00}", $ts.Elapsed.Hours, $ts.Elapsed.Minutes, $ts.Elapsed.Seconds, $ts.Elapsed.Milliseconds);
    return $elapsedTime
}

#-----------------------------------------------------------------------------------------------------------#
function checkSQLConnectivity($instanceName, $databaseName)
{
    #check connectivity to SQL instance
    $connectionString = "Data Source=$instanceName;Initial Catalog=$databaseName;Connect Timeout=30;"

    if (-not([string]::IsNullOrEmpty($sqlLoginName)))
    {

        $connectionString+='User ID='
        $connectionString+=$sqlLoginName
        $connectionString+='; Password='
        $connectionString+=$sqlLoginPassword
        $connectionString+=';'
    }
    else 
    {
        $connectionString+='Integrated Security=true;'
    }

    $sqlConn = new-object ("Data.SqlClient.SqlConnection") $connectionString
    trap
    {
        Exit-Fail "Could NOT connect to instance: $instanceName -> database name: [$databaseName]";
    }
    $sqlConn.Open()
    if ($sqlConn.State -eq 'Open')
    {
        $sqlConn.Close();
        "Connected to SQL instance successfully."
    }
}


#-----------------------------------------------------------------------------------------------------------#
function getSQLServerVersion($intanceName)
{
    $sqlScript = "DECLARE @productVersion [varchar](16)
                    SET @productVersion = CAST(SERVERPROPERTY('ProductVersion') AS [varchar])
                    SELECT @productVersion as version"
    if ([string]::IsNullOrEmpty($sqlLoginName))
    {
        $results = invoke-sqlcmd -ServerInstance $instanceName -Database master -Query "$sqlScript" -Querytimeout $queryTimeout
    }
    else
    {
        $results = invoke-sqlcmd -ServerInstance $instanceName -Database master -Username $sqlLoginName -Password $sqlLoginPassword -Query "$sqlScript" -Querytimeout $queryTimeout
    }


    foreach ($row in $results)
    {
        $productVersion = $row.version;
    }
    return $productVersion
}


#-----------------------------------------------------------------------------------------------------------#
function checkRepositoryDatabase($intanceName)
{
    $sqlScript = "
DECLARE   @appVersion	[sysname]
		, @queryToRun	[nvarchar](4000)
		, @queryParam	[nvarchar](512)
		, @configTableExists [bit]

IF EXISTS(SELECT * FROM sys.databases WHERE [name]='$databaseName')	
	begin
		SET @configTableExists = 0
		SET @queryToRun = 'SELECT @configTableExists=1 FROM [$databaseName].INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=''appConfigurations'''
		SET @queryParam = '@configTableExists [bit] OUTPUT'
		EXEC sp_executesql @queryToRun, @queryParam, @configTableExists = @configTableExists OUT

		IF @configTableExists = 1	
			begin
				SET @queryToRun = 'SELECT @appVersion = [value] FROM [$databaseName].[dbo].[appConfigurations] WHERE [module] = ''common'' AND [name] = ''Application Version'''
				SET @queryParam = '@appVersion [sysname] OUTPUT'
				EXEC sp_executesql @queryToRun, @queryParam, @appVersion = @appVersion OUT
			end
		SELECT REPLACE(@appVersion, '.', '') AS appVersion
	end"
    if ([string]::IsNullOrEmpty($sqlLoginName))
    {
        $results = invoke-sqlcmd -ServerInstance $instanceName -Database master -Query "$sqlScript" -Querytimeout $queryTimeout
    }
    else
    {
        $results = invoke-sqlcmd -ServerInstance $instanceName -Database master -Username $sqlLoginName -Password $sqlLoginPassword -Query "$sqlScript" -Querytimeout $queryTimeout
    }


    foreach ($row in $results)
    {
        $appVersion = $row.appVersion;
    }
    return $appVersion
}


#-----------------------------------------------------------------------------------------------------------#
function createDatabaseIfMissing()
{
    $sqlScript = "
IF NOT EXISTS(SELECT * FROM sys.databases WHERE [name] = '$databaseName')
	begin
		DECLARE   @dataFilePath		[nvarchar](260)
				, @logFilePath		[nvarchar](260)
				, @queryToRun		[nvarchar](4000)
				, @engineEdition	[int]
				, @serverVersionStr	[sysname]
				, @serverVersionNum	[numeric](9,6)
				, @hostPlatform		[sysname]


		SET @dataFilePath = '$dataFilePath'
		SET @logFilePath = '$logFilePath'
		SET @engineEdition = CAST(SERVERPROPERTY('EngineEdition') AS [int])

		SELECT @serverVersionStr = CAST(SERVERPROPERTY('ProductVersion') AS [sysname]) 
		SET @serverVersionNum=SUBSTRING(@serverVersionStr, 1, CHARINDEX('.', @serverVersionStr)-1) + '.' + REPLACE(SUBSTRING(@serverVersionStr, CHARINDEX('.', @serverVersionStr)+1, LEN(@serverVersionStr)), '.', '') 

		IF @serverVersionNum >= 14
			begin
				SET @queryToRun = N'SELECT [host_platform] FROM sys.dm_os_host_info'

				IF object_id('tempdb..#tmpOutput') IS NOT NULL 
				DROP TABLE #tmpOutput

				CREATE TABLE #tmpOutput
				(
					[output] [nvarchar](512) NULL
				)

				INSERT	INTO #tmpOutput([output])
						EXEC sp_executesql @queryToRun

				SELECT @hostPlatform = LOWER([output])
				FROM #tmpOutput
			end

		/* try to read default data and log file location from registry */
		IF ISNULL(@dataFilePath, '')='' AND @engineEdition NOT IN (5, 6, 8) AND NOT (@serverVersionNum >= 14 AND @hostPlatform='linux' ) 
			EXEC master.dbo.xp_instance_regread   N'HKEY_LOCAL_MACHINE'
												, N'Software\Microsoft\MSSQLServer\MSSQLServer'
												, N'DefaultData'
												, @dataFilePath output;

		IF ISNULL(@logFilePath, '')='' AND @engineEdition NOT IN (5, 6, 8) AND NOT (@serverVersionNum >= 14 AND @hostPlatform='linux' ) 
			EXEC master.dbo.xp_instance_regread	  N'HKEY_LOCAL_MACHINE'
												, N'Software\Microsoft\MSSQLServer\MSSQLServer'
												, N'DefaultLog'
												, @logFilePath output;

        IF ISNULL(@dataFilePath, '')<>'' AND RIGHT(@dataFilePath, 1)<>'\' SET @dataFilePath = @dataFilePath + '\'
		IF ISNULL(@logFilePath, '')<>'' AND RIGHT(@logFilePath, 1)<>'\'	SET @logFilePath = @logFilePath + '\'

		IF @engineEdition NOT IN (5, 6, 8) AND ISNULL(@dataFilePath, '')<>'' AND ISNULL(@logFilePath, '')<>''
			SET @queryToRun = N'CREATE DATABASE [$databaseName] ON PRIMARY 
			( NAME = N''" + $databaseName + "_data'', FILENAME = ''' + @dataFilePath + N'" + $databaseName + "_data.mdf'' , SIZE = 32MB , MAXSIZE = UNLIMITED, FILEGROWTH = 64MB )
			 LOG ON 
			( NAME = N''" + $databaseName + "_log'', FILENAME = ''' + @logFilePath + N'" + $databaseName + "_log.ldf'' , SIZE = 32MB , MAXSIZE = UNLIMITED , FILEGROWTH = 64MB)'
		ELSE
			SET @queryToRun = N'CREATE DATABASE [$databaseName]'
		EXEC sp_executesql @queryToRun

		IF @engineEdition NOT IN (5, 6, 8)
			begin
				SET @queryToRun=N'ALTER DATABASE [$databaseName] SET RECOVERY SIMPLE'
				EXEC sp_executesql  @queryToRun

				SET @queryToRun=N'ALTER DATABASE [$databaseName] SET ALLOW_SNAPSHOT_ISOLATION ON'
				EXEC sp_executesql  @queryToRun

				SET @queryToRun=N'ALTER DATABASE [$databaseName] SET READ_COMMITTED_SNAPSHOT ON'
				EXEC sp_executesql  @queryToRun
			end
	end"

    try
    {
		Write-Host "	* Creating repository database [$databaseName] on instance [$instanceName]"
        if ([string]::IsNullOrEmpty($sqlLoginName))
        {
            $results = invoke-sqlcmd -ServerInstance $instanceName -Database master -Query "$sqlScript" -Querytimeout $queryTimeout
        }
        else
        {
            $results = invoke-sqlcmd -ServerInstance $instanceName -Database master -Username $sqlLoginName -Password $sqlLoginPassword -Query "$sqlScript" -Querytimeout $queryTimeout
        }    	
    }
	Catch
    {
		Exit-Fail "Failed to create Repo database -> $database : $_.Exception.Message"
	}
}


#-----------------------------------------------------------------------------------------------------------#
function dropRepositoryDatabase
{
    $sqlScript = " ALTER DATABASE [$databaseName] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                   DROP DATABASE [$databaseName];"

    try
    {
		Write-Host "Dropping repository database: [$databaseName]"
        if ([string]::IsNullOrEmpty($sqlLoginName))
        {
            $results = invoke-sqlcmd -ServerInstance $instanceName -Database master -Query "$sqlScript" -Querytimeout $queryTimeout
        }
        else
        {
            $results = invoke-sqlcmd -ServerInstance $instanceName -Database master -Username $sqlLoginName -Password $sqlLoginPassword -Query "$sqlScript" -Querytimeout $queryTimeout
        }    	
    }
	Catch
    {
		Exit-Fail "Failed to drop database -> $database : $_.Exception.Message"
	}
}


#-----------------------------------------------------------------------------------------------------------#
function dropSQLAgentJobs()
{
    $sqlScript = "
DECLARE   @jobName		[sysname]
		, @strMessage	[nvarchar](1024)
		
DECLARE crtSQLServerAgentJobs CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT sj.[name]
															FROM [msdb].[dbo].[sysjobs] sj
															INNER JOIN [msdb].[dbo].[sysjobsteps] sjs ON sjs.[job_id]=sj.[job_id]
															WHERE sjs.[database_name] = '$databaseName'
																OR sj.[name] LIKE ('$databaseName' + '%')
OPEN crtSQLServerAgentJobs
FETCH NEXT FROM crtSQLServerAgentJobs INTO @jobName
WHILE @@FETCH_STATUS=0	
	begin
		IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = @jobName)
			AND EXISTS(
						--check if the job is running
						SELECT * 
						FROM (	
						SELECT B [step_id], SUBSTRING(A, 7, 2) + SUBSTRING(A, 5, 2) + SUBSTRING(A, 3, 2) + LEFT(A, 2) + '-' + SUBSTRING(A, 11, 2) + SUBSTRING(A, 9, 2) + '-' + SUBSTRING(A, 15, 2) + SUBSTRING(A, 13, 2) + '-' + SUBSTRING(A, 17, 4) + '-' + RIGHT(A , 12) [job_id] 
						FROM	(
								 SELECT SUBSTRING([program_name], CHARINDEX(': Step', [program_name]) + 7, LEN([program_name]) - CHARINDEX(': Step', [program_name]) - 7) B, SUBSTRING([program_name], CHARINDEX('(Job 0x', [program_name]) + 7, CHARINDEX(' : Step ', [program_name]) - CHARINDEX('(Job 0x', [program_name]) - 7) A
	 							 FROM [master].[dbo].[sysprocesses] 
	 							 WHERE [program_name] LIKE 'SQLAgent - %JobStep%') A
								) A 
						WHERE [job_id] IN (
											SELECT DISTINCT [job_id] 
											FROM [msdb].[dbo].[sysjobs] 
											WHERE [name]= @jobName
										  )
						  )
			begin
				SET @strMessage = 'Stop job: ' + @jobName
				PRINT @strMessage
				EXEC msdb.dbo.sp_stop_job   @job_name = @jobName
			end
	
		SET @strMessage = 'Delete job: ' + @jobName
		PRINT @strMessage

		EXEC msdb.dbo.sp_delete_job   @job_name=@jobName

		FETCH NEXT FROM crtSQLServerAgentJobs INTO @jobName
	end
CLOSE crtSQLServerAgentJobs
DEALLOCATE crtSQLServerAgentJobs"

    try
    {       
		Write-Host "Stop and drop existing SQL Agent jobs for database [$databaseName]"
        if ([string]::IsNullOrEmpty($sqlLoginName))
        {
            $results = invoke-sqlcmd -ServerInstance $instanceName -Database master -Query "$sqlScript" -Querytimeout $queryTimeout
        }
        else
        {
            $results = invoke-sqlcmd -ServerInstance $instanceName -Database master -Username $sqlLoginName -Password $sqlLoginPassword -Query "$sqlScript" -Querytimeout $queryTimeout
        }    	
    }
	Catch
    {
		Exit-Fail "Failed to clean the SQL Agent jobs for database -> $database : $_.Exception.Message"
	}
}


#-----------------------------------------------------------------------------------------------------------#
#main
#-----------------------------------------------------------------------------------------------------------#

#verifying input parameters
if (("$module".ToLower() -ne "all") -and ("$module".ToLower() -ne "health-check") -and ("$module".ToLower() -ne "maintenance-plan"))
{
    Exit-Fail "Current value for module parameter is not allowed. Accepted options are: all | health-check | maintenance-plan"
    exit;
}

if (($useParallelExecution.ToLower() -ne "y") -and ($useParallelExecution.ToLower() -ne "yes") -and ($useParallelExecution.ToLower() -ne "n") -and ($useParallelExecution.ToLower() -ne "no"))
{
    Exit-Fail "Current value for useParallelExecution parameter is not allowed. Accepted options are: Y | Yes | N | No"
    exit;
}
if ($useParallelExecution.ToLower() -eq "y")
{
    $useParallelExecution="yes"
}
if ($useParallelExecution.ToLower() -eq "n")
{
    $useParallelExecution="no"
}

if (($recreateSQLAgentJobs.ToLower() -ne "y") -and ($recreateSQLAgentJobs.ToLower() -ne "yes") -and ($recreateSQLAgentJobs.ToLower() -ne "n") -and ($recreateSQLAgentJobs.ToLower() -ne "no"))
{
    Exit-Fail "Current value for recreateSQLAgentJobs parameter is not allowed. Accepted options are: Y | Yes | N | No"
    exit;
}
if ($recreateSQLAgentJobs.ToLower() -eq "y")
{
    $recreateSQLAgentJobs="yes"
}
if ($recreateSQLAgentJobs.ToLower() -eq "n")
{
    $recreateSQLAgentJobs="no"
}

if (($uninstall.ToLower() -ne "y") -and ($uninstall.ToLower() -ne "yes") -and ($uninstall.ToLower() -ne "n") -and ($uninstall.ToLower() -ne "no"))
{
    Exit-Fail "Current value for uninstall parameter is not allowed. Accepted options are: Y | Yes | N | No"
    exit;
}
if ($uninstall.ToLower() -eq "y")
{
    $uninstall="yes"
}
if ($uninstall.ToLower() -eq "n")
{
    $uninstall="no"
}


#-----------------------------------------------------------------------------------------------------------#
#check connectivity to SQL instance
checkSQLConnectivity $instanceName $master

#get SQL Server version
$productVersion = ""
$productVersion = getSQLServerVersion $instanceName
Write-Host "Detected SQLServer version: $productVersion"


#check if repository database exists
$appVersion =""
$appVersion = checkRepositoryDatabase

if ([string]::IsNullOrEmpty($appVersion))
{
    $actioneMode = "install"
    $recreateSQLAgentJobs="yes"
}
else
{
    $actioneMode = "upgrade"
}


#checking is uninstall is selected
if ($uninstall.ToLower() -eq "yes")
{
    Write-Host "#-----------------------------------------------------------------------------------#"
    Write-Host "Performing utility UNINSTALL..."

    if ($actioneMode.ToLower() -eq "install")
    {
        dropSQLAgentJobs;
        Write-Host "Database [$databaseName] was not found on the instance."
    }
    else
    {
        dropSQLAgentJobs;
        dropRepositoryDatabase;
    }
    exit;
}


#create repository database if it does not exists
if ("$actioneMode".ToLower() -eQ "install")
{
    Write-Host "Performing utility INSTALL..."
    createDatabaseIfMissing
}
else
{
    Write-Host "Detected dbaTDPMon version: $appVersion. Performing UPGRADE."
}

if ($actioneMode -eq "upgrade") 
{
    if( [int]$appVersion -lt 20190712)
    {
        Exit-Fail ("You must upgrade to dbaTDPMon version 2019.7 before upgrading to last version.")
    }
}

Write-Host "	* Configuring using parallel execution mode:" $useParallelExecution.ToUpper()
Write-Host "	* Recreating SQL Agent jobs:" $recreateSQLAgentJobs.ToUpper()


if ($recreateSQLAgentJobs.ToLower() -eq "yes")
{
    dropSQLAgentJobs;
}


#-----------------------------------------------------------------------------------------------------------#
# detecting modules to install/upgrade
$moduleList = @()
$moduleList_counter ++
$moduleList += ,@($moduleList_counter, 'common', 'common')

if (($module.ToLower() -eq "maintenance-plan") -or ($module.ToLower() -eq "all"))
{
    $moduleList_counter ++
    $moduleList += ,@($moduleList_counter, 'maintenance-plan', 'mp')
}

if (($module.ToLower() -eq "health-check") -or ($module.ToLower() -eq "all"))
{
    $moduleList_counter ++
    $moduleList += ,@($moduleList_counter, 'health-check', 'hc')
    $moduleList_counter ++
    $moduleList += ,@($moduleList_counter, 'monitoring', 'mon')
}
if (($module.ToLower() -eq "all"))
{
    $moduleList_counter ++
    $moduleList += ,@($moduleList_counter, 'auto-heal', 'ah')
}


#-----------------------------------------------------------------------------------------------------------#
foreach($moduleDetail in $moduleList)
{
    $moduleName = $moduleDetail.Item(1)
    $moduleCode = $moduleDetail.Item(2)

    Write-Host "#-----------------------------------------------------------------------------------#"
    Write-Host "# module: $moduleName"
    Write-Host "#-----------------------------------------------------------------------------------#"

    Write-Host "# deploying: SCHEMAS"
    foreach ($f in Get-ChildItem -path ".\$moduleName\schema\" -filter *.sql | sort-object)
    {
        $scriptName = $f.FullName;
        $message = "	* " + $f.Name
        Write-Host $message

        if ([string]::IsNullOrEmpty($sqlLoginName))
        {
            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
        }
        else
        {
            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
        }

    }


    #tables are being created only during installation
    Write-Host "# deploying: TABLES"
    if ("$actioneMode".ToLower() -eq "install")
    {
        if ($moduleName.ToLower() -eq "common")
        {
            #create categories tables first, as referential constraints are defined for some tables
            $fileList="dbo.catalogSolutions.sql","dbo.catalogProjects.sql","dbo.catalogMachineNames.sql","dbo.catalogInstanceNames.sql","dbo.catalogDatabaseNames.sql"
            foreach ( $fileName in $fileList )
            {
                $message = "	* " + $fileName
                $scriptName = (Get-Location).Path + "\$moduleName\tables\" + $($fileName)
                Write-Host $message

                if ([string]::IsNullOrEmpty($sqlLoginName))
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
                }
                else
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
                }
            }
        }

        foreach ($f in Get-ChildItem -path ".\$moduleName\tables\" -filter *.sql | sort-object)
        {
            $scriptName = $f.Name;
            if ($fileList -NotContains "$scriptName")  
            {
                $scriptName = $f.FullName;
                $message = "	* " + $f.Name
                Write-Host $message

                if ([string]::IsNullOrEmpty($sqlLoginName))
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
                }
                else
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
                }
            }
        }
    }

    #run script creation for the new tables
    if ("$actioneMode".ToLower() -eq "upgrade")
    {
        foreach ($f in Get-ChildItem -path ".\$moduleName\tables\" -filter *.sql | sort-object)
        {
            $scriptName = $f.Name

            #check if table exists
            $sqlScript = "SELECT COUNT(*) AS [table_exists] FROM sys.tables WHERE OBJECT_SCHEMA_NAME([object_id]) + '.' + OBJECT_NAME([object_id]) + '.sql' = '$scriptName'"
        
            if ([string]::IsNullOrEmpty($sqlLoginName))
            {
                $results = invoke-sqlcmd -ServerInstance $instanceName -Database $databaseName -Query "$sqlScript" -Querytimeout $queryTimeout
            }
            else
            {
                $results = invoke-sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -Query "$sqlScript" -Querytimeout $queryTimeout
            }

            $tableExists = $results.table_exists

            if ("$tableExists" -eq "0")
            {
                $scriptName = $f.FullName;
                $message = "	* " + $f.Name
                Write-Host $message

                if ([string]::IsNullOrEmpty($sqlLoginName))
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
                }
                else
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
                }
            }
        }
    }


    #run the upgrade-scripts for altering table's structure or data
    if ("$actioneMode".ToLower() -eq "upgrade")
    {
        Write-Host "# running upgrade scripts"

        foreach ($f in Get-ChildItem -path ".\patches\" -filter *-$moduleCode.sql | sort-object)
        {
            $scriptName = $f.FullName;
            $message = "	* " + $f.Name
            $scriptDate = ($f.Name).Substring(0,8)

            if ([int]$scriptDate -gt [int]$appVersion)
            {
                Write-Host $message

                if ([string]::IsNullOrEmpty($sqlLoginName))
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
                }
                else
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
                }
            }
        }
    }


    Write-Host "# deploying: VIEWS"
    if ($moduleName.ToLower() -eq "common")
    {
        $fileList="dbo.ufn_getObjectQuoteName.sql"
        foreach ( $fileName in $fileList )
        {
            $message = "	* " + $fileName
            $scriptName = (Get-Location).Path + "\$moduleName\functions\" + $($fileName)
            Write-Host $message

            if ([string]::IsNullOrEmpty($sqlLoginName))
            {
                Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
            }
            else
            {
                Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
            }
        }
    }

    foreach ($f in Get-ChildItem -path ".\$moduleName\views\" -filter *.sql | sort-object)
    {
        $scriptName = $f.FullName;
        $message = "	* " + $f.Name
        Write-Host $message

        if ([string]::IsNullOrEmpty($sqlLoginName))
        {
            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
        }
        else
        {
            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
        }

    }


    Write-Host "# deploying: FUNCTIONS"
    foreach ($f in Get-ChildItem -path ".\$moduleName\functions\" -filter *.sql | sort-object)
    {
        $scriptName = $f.FullName;
        $message = "	* " + $f.Name
        Write-Host $message

        if ([string]::IsNullOrEmpty($sqlLoginName))
        {
            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
        }
        else
        {
            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
        }

    }


    Write-Host "# deploying: STORED PROCEDURES"
    foreach ($f in Get-ChildItem -path ".\$moduleName\stored-procedures\" -filter *.sql | sort-object)
    {
        $scriptName = $f.FullName;
        $message = "	* " + $f.Name
        Write-Host $message

        if ([string]::IsNullOrEmpty($sqlLoginName))
        {
            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
        }
        else
        {
            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true
        }

    }

    #default configurations are set only at install time
    if ("$actioneMode".ToLower() -eq "install")
    {
        Write-Host "# configuring module defaults"
        foreach ($f in Get-ChildItem -path ".\$moduleName\" -filter *.sql | sort-object)
        {
            $scriptName = $f.FullName;
            $message = "	* " + $f.Name
            Write-Host $message

            if ([string]::IsNullOrEmpty($sqlLoginName))
            {
                Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName")
            }
            else
            {
                Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName")
            }

        }
    }
}

#default configurations are set only at install time
if ("$actioneMode".ToLower() -eq "install")
{
    Write-Host "#-----------------------------------------------------------------------------------#"
    Write-Host "# configuring utility defaults"
    foreach ($f in Get-ChildItem -path ".\setup\" -filter *.sql | sort-object)
    {
        $scriptName = $f.FullName;
        $message = "	* " + $f.Name
        Write-Host $message

        if ([string]::IsNullOrEmpty($sqlLoginName))
        {
            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName")
        }
        else
        {
            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName")
        }

    }
}

if ($recreateSQLAgentJobs.ToLower() -eq "yes")
{
    Write-Host "#-----------------------------------------------------------------------------------#"
    Write-Host "# deploying SQL Agent jobs"
    foreach($moduleDetail in $moduleList)
    {
        $moduleName = $moduleDetail.Item(1)
        if (($moduleName -eq "health-check") -or ($moduleName -eq "monitoring"))
        {
            foreach ($f in Get-ChildItem -path ".\$moduleName\job-scripts\" -filter *.sql | sort-object)
            {
                $scriptName = $f.FullName;
                $message = "	* " + $f.Name
                Write-Host $message

                if ([string]::IsNullOrEmpty($sqlLoginName))
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName","dbName=$databaseName")
                }
                else
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName","dbName=$databaseName")
                }
            }
        }

        if (($moduleName -eq "maintenance-plan"))
        {
            foreach ($f in Get-ChildItem -path ".\$moduleName\job-scripts\" -filter msdb-create-custom-indexes.sql  | sort-object)
            {
                $scriptName = $f.FullName;
                $message = "	* " + $f.Name
                Write-Host $message

                if ([string]::IsNullOrEmpty($sqlLoginName))
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName","dbName=$databaseName")
                }
                else
                {
                    Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName","dbName=$databaseName")
                }
            }


            if ($useParallelExecution.ToLower() -eq "no")
            {
                #jobs that will run in a "serial" fashion
                foreach ($f in Get-ChildItem -path ".\$moduleName\job-scripts\" -filter *job-script*.sql  | sort-object)
                {
                    if ($f.Name -ne "job-script-dbaTDPMon - Database Maintenance - System DBs - remote.sql")
                    {
                        $scriptName = $f.FullName;
                        $message = "	* " + $f.Name
                        Write-Host $message

                        if ([string]::IsNullOrEmpty($sqlLoginName))
                        {
                            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName","dbName=$databaseName")
                        }
                        else
                        {
                            Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName","dbName=$databaseName")
                        }
                    }
                }
            }
            else
            {
                #jobs that can run in parallel, for multiple instances / databases if needed
                $message = "	* backup and maintenance jobs for all instances/databases under project: " + $projectName
                Write-Host $message

                $sqlScript = "EXEC [dbo].[usp_mpJobProjectDefaultPlanCreate] @projectCode = '$projectName', @sqlServerNameFilter = @@SERVERNAME;"
                if ([string]::IsNullOrEmpty($sqlLoginName))
                {
                    Invoke-SqlCmd -ServerInstance $instanceName -Database $databaseName -Query "$sqlScript" -Querytimeout $queryTimeout
                }
                else
                {
                    Invoke-SqlCmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -Query "$sqlScript" -Querytimeout $queryTimeout
                }
            
                foreach ($f in Get-ChildItem -path ".\$moduleName\job-scripts\" -filter *job-script*remote.sql | sort-object)
                {
                    $scriptName = $f.FullName;
                    $message = "	* " + $f.Name
                    Write-Host $message

                    if ([string]::IsNullOrEmpty($sqlLoginName))
                    {
                        Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName","dbName=$databaseName")
                    }
                    else
                    {
                        Invoke-Sqlcmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -inputFile $scriptName -Querytimeout $queryTimeout -OutputSqlErrors $true -Variable @("projectCode=$projectName","dbName=$databaseName")
                    }
                }
            }
        }
    }
}


Write-Host "#-----------------------------------------------------------------------------------#"
#save installed information
$sqlScript = "SET NOCOUNT ON; UPDATE [dbo].[appConfigurations] SET [value] = N'2023.10.18' WHERE [module] = 'common' AND [name] = 'Application Version'"
if ([string]::IsNullOrEmpty($sqlLoginName))
{
    Invoke-SqlCmd -ServerInstance $instanceName -Database $databaseName -Query "$sqlScript" -Querytimeout $queryTimeout
}
else
{
    Invoke-SqlCmd -ServerInstance $instanceName -Database $databaseName -Username $sqlLoginName -Password $sqlLoginPassword -Query "$sqlScript" -Querytimeout $queryTimeout
}

$appVersion = checkRepositoryDatabase
Write-Host "Deployed dbaTDPMon version: $appVersion."

$analysisStopWatch.Stop();
Write-Host "Done."
formatElapsedTime $analysisStopWatch
