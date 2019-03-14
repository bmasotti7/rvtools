#region: ############################# >>> Import/Instantiate Functions

########## >> SQL Functions:
$Script = Get-Item -Path "\\winshare.cifs.gac.gulfaero.com\users\u331285\PowerShell\SQL\SQL-Functions.ps1"; $LocalPath = "C:\Program Files\WindowsPowerShell"
Copy-Item -Path $Script.FullName -Destination $LocalPath -Force; . "$LocalPath\$($Script.Name)"

########## >> Write-LogEvent:
Function Write-LogEvent {
    [CmdletBinding()]
    PARAM (
        [Parameter(Mandatory=$True,Position=1)][String]$Message,
        [Parameter(Mandatory=$False,Position=2)][Int]$Level = 4,
        [Parameter(Mandatory=$False,Position=3)]$Script,
        [Parameter(Mandatory=$False,Position=4)]$Type = $Type,
        [String]$LogPath = "\\winshare.cifs.gac.gulfaero.com\monitoring\Logs\Scripted",
        [String]$File = "EventLog.csv"
    )

    $Event = '' | Select DateTime,Computer,Script,Type,Level,Message

    If ($Script -eq $null) {$Script = $psISE.CurrentFile.DisplayName.Split(".")[0]}
    If ($Type -eq $null) {
        $Type = Switch -RegEx ($Script) { "SQL"{"SQL"} "Log|Monitor|Parse"{"Monitoring"} "Sync"{"Data Sync"} Default{"Misc"} }
    }

    $Event.DateTime = Get-Date -Format "MM/dd/yy HH:mm:ss"
    $Event.Computer = $env:COMPUTERNAME
    $Event.Script = $Script
    $Event.Type = $Type
    $Event.Level = Switch ($Level) { '1'{'Failure'} '2'{'Error'} '3'{'Warning'} '4'{'Info'} }
    $Event.Message = $Message

    $LogExt = $File.Split(".") | Select -Last 1
    If ($LogExt -match 'csv|log') {$Event | Export-Csv -Path "$LogPath\$File" -Append -NoTypeInformation}
    ElseIf ($LogExt -match 'txt') {$Event | Add-Content -Path "$LogPath\$File"}
    "$($Event.DateTime):`t$($Event.Message)."
}
Set-Alias -Name WLE -Value Write-LogEvent

########## >> Excel Module:
Import-Module ImportExcel

#endregion: ############################# >>> Import/Instantiate Functions


#region: ############################# >>> Set Variables

########## >> File Paths/Names:
$FilePath = "\\winshare.cifs.gac.gulfaero.com\monitoring\Reports\Import_Archives"
$SQLPath = "\\winshare.cifs.gac.gulfaero.com\users\u331285\PowerShell\SQL"
$SQLImportLog = "\\winshare.cifs.gac.gulfaero.com\monitoring\Logs\Scripted\SQLImport-Log.csv"
$FailPath = "\\winshare.cifs.gac.gulfaero.com\monitoring\Reports\Import_Archives\RVTools\Archive\FailedImports"

##### >> Create a temporary staging path to aggregate and format the data to import:
$Drive = If (Test-Path -Path "F:") {"F:"} ElseIf (Test-Path -Path "E:") {"E:"} Else {"C:"}
$TempPath = "$Drive\Temp\SQLStaging"
If (!(Test-Path $TempPath)) {New-Item -ItemType Directory -Path "$TempPath" | Out-Null}

########## >> Column Mapping:
$ColumnMapping = Import-Excel -Path "$SQLPath\SQL_Tables.xlsx"

########## >> Optional: Key Columns: Set the Primary Date Key (to query rows using a specific date field):
$PrimaryDateKey = 'ReportDate'

########## >> Data sets to sync: a data set consists of a database and the tables, columns, and datatypes of a particular set:
$DataSets = $ColumnMapping.DataSet | Sort -Unique

#$ErrorActionPreference = "Inquire" # $ErrorActionPreference = "Continue"

#endregion: ############################# >>> Set Variables


#region: ############################# >>> Parse/Format/Import Data Into SQL

$DataSets = $DataSets | Where {$_ -eq "RVTools"} ### > Temp $DataSet assignment

ForEach ($DataSet in $DataSets) {

    $TempPath = "$TempPath\$DataSet"
    If (!(Test-Path $TempPath)) {New-Item -ItemType Directory -Path "$TempPath" | Out-Null}

    ########## >> List of databases in the $DataSet:
    $Databases = $ColumnMapping | Where {$_.DataSet -eq $DataSet} | Select -ExpandProperty Database | Sort -Unique

    $Results = @(); $j = 0

    ForEach ($Database in $Databases) {

        ########## >> List of SQL tables to sync:
        [Array]$Tables = $ColumnMapping | Where {$_.Database -eq $Database} | Select -ExpandProperty Table -Unique
        <#$Tables = "RVTools_CD","RVTools_CPU","RVTools_Floppy","RVTools_Partition","RVTools_Snapshot","RVTools_Tools","RVTools_Cluster","RVTools_HBA",
        "RVTools_Health","RVTools_MultiPath","RVTools_NIC","RVTools_Port","RVTools_RP","RVTools_SC_VMK","RVTools_Switch","RVTools_dvSwitch","RVTools_dvPort" #>

        ########## >> Destination Database Connection:
        $SQLServer = "INFSQLPW002"
        $SqlConnection = New-Object System.Data.SqlClient.SqlConnection; $SqlCmd = New-Object System.Data.SqlClient.SqlCommand; $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
        $SqlConnection.ConnectionString = "Server = $SQLServer; Database = $Database; Integrated Security = True"; $SqlCmd.Connection = $SqlConnection; $SqlAdapter.SelectCommand = $SqlCmd


        ForEach ($Table in $Tables) { # > $Table = $Tables[$j]; $j++; $Table

            $StartTime = Get-Date

            ########## >> Database table column to CSV file column mapping:
            $ColumnMap = $ColumnMapping | Where {$_.Database -eq $Database -and $_.Table -eq $Table}

            $SelectString = ForEach ($Column in $ColumnMap) {
                If ($Column.Column -eq "ReportFile") {"@{N='ReportFile';E={`$ImportFile.Name}}"}
                ElseIf ($Column.SourceColumn -eq "LastWriteTime") {"@{N='ReportDate';E={`$ImportFile.LastWriteTime}}"}
                ElseIf ($Column.SourceColumn -eq "LastWriteDate") {"@{N='ReportDate';E={Get-Date (`$ImportFile.LastWriteTime) -Format 'yyyy-MM-dd'}}"}
                ElseIf ($Column.SourceColumn -eq "vCenter") {"@{N='vCenter';E={`$ImportFile.DirectoryName.Split('\\') | Select -Last 1}}"}
                Else {"@{N='$($Column.Column)';E={`$_.'$($Column.SourceColumn)'}}"}
            }
            $SelectString = $SelectString -join ","

            $DateKey = $ColumnMap | Where {$_.Column -match "$PrimaryDateKey"} #| Select -ExpandProperty Column | Select -First 1
            $ImportType = $ColumnMap.ImportType | Sort -Unique

            ########## >> Source: Location of the files to import:
            $Source = $ColumnMap | Select -ExpandProperty Source -Unique
            $SourceTable = $ColumnMap.SourceTable[0]


            ########## >> Optional: Archive (Copy) the $ImportFiles to a location designated in $ArchivePath:
            If ($Table -match "RVTools") {
                $ArchivePath = Switch -Regex ($Table) {"RVTools"{"$FilePath\RVTools"}}
                $ImportFiles = Get-ChildItem -File -Recurse -Path $Source -Filter "*$SourceTable*"
                ForEach ($ImportFile in $ImportFiles) {
                    $FileDate = Get-Date($ImportFile.LastWriteTime.ToLongDateString()) -Format "MM-dd-yy"
                    $vCenter = $ImportFile.DirectoryName.Split("\\") | Select -Last 1
                    $FileName = "$ArchivePath\$vCenter\$($ImportFile.BaseName -replace "(tabv|tab)")_$vCenter`_$FileDate$($ImportFile.Extension)"
                    Try {Copy-Item -Path $ImportFile.FullName -Destination $FileName -Force}
                    Catch {
                        If (!(Test-Path -Path "$ArchivePath\$vCenter")) {New-Item -ItemType Directory -Path "$ArchivePath\$vCenter" | Out-Null}
                        Copy-Item -Path $ImportFile.FullName -Destination $FileName -Force
                    }
                }
                $Source = $ArchivePath
            }
        

            ########## >> Reports will be selected if their file name does not appear in the corresponding file column in the destination table:
            If ($ImportType -eq "File") {
                $ImportedFiles = Query-SQL "Select Distinct [ReportFile] From [$Table] Order By [ReportFile]" | Select -ExpandProperty ReportFile
                $ImportFiles = Get-ChildItem -File -Path $Source -Filter "*$SourceTable*" | Where {$ImportedFiles -notcontains $_.Name}
            }
            ########## >> Rows will be selected if their DateTime values do not match any existing records in the database:
            ElseIf ($ImportType -eq "Row") {
                $LastSyncTime = Query-SQL "Select Distinct Top 1 [$($DateKey.Column)] From [$Table] Order By [$($DateKey.Column)] Desc" | Select -ExpandProperty $DateKey.Column
                ##### >> Get files with a LastWriteTime greater than $LastSyncTime:
                $SyncFiles = Get-ChildItem -File -Path $Source -Filter "*$SourceTable*" | Sort LastWriteTime | Select -Last 1
                $ImportFiles = $SyncFiles | ForEach {If ((Get-Date(Get-Date $_.LastWriteTime -Format f)) -gt $LastSyncTime) {$_}}
            }
            ########## >> Reports will be selected if their LastWriteTime values do not match any existing records in the database:
            ElseIf ($ImportType -eq "LastWriteTime") {
                ##### >> Attempt to determine the column with the record/import date values:
                $ReportDateColumn = $DateKey.SourceColumn; $DateColumn = $DateKey.Column
                If ($DateColumn -eq $null) {$DateColumn = Derive-DateColumn $Table}
                ##### >> Get all datetime values currently in the SQL table:
                $Dates = Query-SQL "Select Distinct [$DateColumn] From [$Table] Order By [$DateColumn]" | Select -ExpandProperty $DateColumn
                $ImportFiles = Get-ChildItem -File -Path $Source -Recurse -Include "*$Table*","*$SourceTable*" |
                Where {$Dates -notcontains (Get-Date (Get-Date $_.LastWriteTime).AddDays(-0) -Format "yyyy-MM-dd")} | Sort LastWriteTime
            }
            ########## >> Else get all files matching the name of the $SourceTable:
            Else {$ImportFiles = Get-ChildItem -File -Path $Source -Recurse -Filter "*$SourceTable*" | Where {$_.Length -ne 0} | Sort LastWriteTime}


            If ($ImportFiles.Count -eq 0) {
                ########## >> Write detailed results to an external log:
                $EndTime = Get-Date ; $ProcessTime = "$([Math]::Round(($EndTime - $StartTime).TotalSeconds,2))"
                $NewRowCount = Get-RowCount -Table $Table; $BeginRowCount = $NewRowCount; $RowsToImport = 0
                $RecordsImported = $NewRowCount - $BeginRowCount
                $RecordsMissed = ($BeginRowCount + $RowsToImport) - $NewRowCount
                $SuccessRate = 'n/a'
                [Array]$FileString = (($ImportFiles.Name -join "`n")).Trim()
                $Results += $Table | Select `
                @{N="DateTime";E={Get-Date -Format "MM/dd/yy HH:mm:ss"}},@{N="Database";E={$Database}},@{N="Table";E={$_}},@{N="RowsToImport";E={$RowsToImport}},
                @{N="BeginRowCount";E={$BeginRowCount}},@{N="NewRowCount";E={$NewRowCount}},@{N="RecordsImported";E={$RecordsImported}},@{N="RecordsMissed";E={$RecordsMissed}},
                @{N="SuccessRate";E={"$SuccessRate"}},@{N="ProcessTime(Sec)";E={$ProcessTime}},@{N="FileCount";E={$ImportFiles.Count}},@{N="FilesImported";E={$FileString}}
                $Results[-1] | Export-Csv -Path $SQLImportLog -Append -NoTypeInformation #>

                "No new data to insert for $Database`: $Table."; Continue
            }


            ########## >> Copy data to a local staging location - it will be formatted and prepared for import into SQL:

            ##### >> Row: If the .csv file has data appended by date, extract only the rows with dates not included in the table's report date column:
            If ($ImportType -eq "Row") {
                ##### >> Attempt to determine the column with the record/import date values:
                $ReportDateColumn = $DateKey.SourceColumn; $DateColumn = $DateKey.Column
                If ($DateColumn -eq $null) {$DateColumn = Derive-DateColumn $Table}
                ##### >> Get all datetime values currently in the SQL table:
                $Dates = Query-SQL "Select Distinct [$DateColumn] From [$Table] Order By [$DateColumn]" | Select -ExpandProperty $DateColumn
                ##### >> Extract rows from the $ImportFile with datetimes not matching any datetimes in the SQL table:
                ForEach ($ImportFile in $ImportFiles) {
                    $TempFile = "$TempPath\$($ImportFile.BaseName)_2$($ImportFile.Extension)"
                    Get-Content -Path $ImportFile.FullName | ForEach {"`"$($_ -replace '"','' -replace ",",'","')`""} | Set-Content -Path $TempFile
                    Import-Csv -Path $TempFile | Where {$Dates -notcontains (Get-Date $_."$ReportDateColumn")} | Select (Invoke-Expression $SelectString) |
                    Export-Csv -Path "$TempPath\$Table.csv" -Append -NoTypeInformation
                    Remove-Item $TempFile -Force
                }
            }
            ElseIf ($Table -eq "App_Launches" -or $Table -eq "XA_Server_Utilization" -or $Table -eq "XA65-Session_Duration") {
                If ($Table -eq "XA65-Session_Duration") {$SelectString = $SelectString -replace "'PublishedApp'}}","'PublishedApp' -replace 'Initial Published Application: '}}"}
                ForEach ($ImportFile in $ImportFiles) {
                    Invoke-Expression "Import-Csv -Path `$ImportFile.FullName | Select $SelectString | Export-Csv -Path '$TempPath\$Table.csv' -Append -NoTypeInformation"
                }
            }
            ElseIf ($Table -eq "RVTools_dvSwitch") {
                ForEach ($ImportFile in $ImportFiles) {
                    $TempFile = "$TempPath\$($ImportFile.BaseName).Temp$($ImportFile.Extension)"
                    ### > Set new column headers for each file:
                    Get-Content -Path $ImportFile.FullName -TotalCount 1 | ForEach {$_ -replace "Contact,Name","Contact,Name2"} | Set-Content -Path $TempFile
                    ### > Add the rest of the file contents to the file with the new header:
                    Get-Content -Path $ImportFile.FullName | Select -Skip 1 | Add-Content -Path $TempFile
                    Import-Csv -Path $TempFile | Select (Invoke-Expression $SelectString) | Export-Csv -Path "$TempPath\$Table.csv" -Append -NoTypeInformation
                    Remove-Item -Path $TempFile -Force
                }
            }
            ########## >> Select only the row with Feature_Code: "XDT_PLT_CCS"
            ElseIf ($Table -eq "Custom_License_Usage_Trend") {
                ForEach ($ImportFile in $ImportFiles) {
                    Import-Csv -Path $ImportFile.FullName | Select @{N='ReportDate';E={$_.'dtperiod'}},
                    @{N='License';E={$_.'feature_code'.Split("`n").Trim() | Select -First 1}},@{N='Feature_Code';E={$_.'feature_code'.Split("`n").Trim() | Select -Last 1}},
                    @{N='Usage/Total';E={$_.'textbox2'}},@{N='ReportFile';E={$ImportFile.Name}} | Where {$_.Feature_Code -eq "XDT_PLT_CCS"} |
                    Export-Csv -Path "$TempPath\$Table.csv" -Append -NoTypeInformation
                }
            }
            Else {ForEach ($ImportFile in $ImportFiles) {Import-Csv -Path $ImportFile.FullName | Select (Invoke-Expression $SelectString) | Export-Csv -Path "$TempPath\$Table.csv" -Append -NoTypeInformation}}


            $DataFiles = Get-ChildItem -Path $TempPath -Filter "*$Table*"
            If ($DataFiles.Count -eq 1) {$DataFile = $DataFiles.FullName}
            Else {
                $DataFile = "$TempPath\$Table.csv"
                $DataFiles | ForEach {Import-Csv -Path $_.FullName | Export-Csv -Path $DataFile -Append -NoTypeInformation}
            }


            $Data = Import-Csv -Path $DataFile

            $Columns = ($Data[0] | ConvertTo-Csv -NoTypeInformation)[0].Replace('"',"").Split(",")
            ForEach ($Column in $Columns) {ForEach ($Row in $Data) {If ($Row."$Column" -eq '' -or $Row."$Column" -eq [DBNull]::Value) {$Row."$Column" = $null}}}

            ############ >> Convert all values to their expected datatype in the destination SQL table:
            ForEach ($Column in ($ColumnMap | Where {$Columns -contains $_.Column})) {
                If ($Column.DataType -match "Date") {
                    ForEach ($Row in $Data) {
                        Try {$Row."$($Column.Column)" = Get-Date $Row."$($Column.Column)"}
                        Catch {$Row."$($Column.Column)" = $null}
                    }
                }
                ElseIf ($Column.DataType -match "int") {
                    ForEach ($Row in $Data) {
                        Try {$Row."$($Column.Column)" = [int]$Row."$($Column.Column)"}
                        Catch {$Row."$($Column.Column)" = $null}
                    }
                }
            }

            ########## >> Use this section to build a string for replacing incompatible SQL characters (e.g., "'"):
            # > Syntax: $Replace = @{"ReplaceThis"="WithThis"}
            $Replace = @{"'"="''"}
            $ReplaceString = ($Replace.Keys | ForEach {"-replace `"$_`",`"$($Replace."$_")`""}) -join " "
            $MatchString = ($Replace.Keys | ForEach {"$_"}) -join "|"


            ########## >> Gets the number of rows currently in the target table and in the file being imported
            $BeginRowCount = Get-RowCount -Table $Table
            $RowsToImport = $Data."$($DateKey.Column)".Count
            "`nTotal records currently in the $Table table: $BeginRowCount`nNumber of rows to import: $RowsToImport"


            $Failed = @()

            ForEach ($Row in $Data) {
                    ##### >> Optional: Use the previously generated ReplaceString to replace incompatible characters:
                    ForEach ($Column in $Columns) {If ($Row."$Column" -match "$MatchString") {$Row."$Column" = Invoke-Expression "(`$Row.'$Column' $ReplaceString).Trim()"}}
                $ColumnString = ($Columns | Where {$Row."$_" -ne $null} | ForEach {"[$_]"}) -join ", "
                $ValueString = ($Columns | Where {$Row."$_" -ne $null} | ForEach {"'$($Row."$_")'"}) -join ", "
                    ##### >> Optional: Insert a "LastUpdate" field into the imported SQL row:
                    #$ColumnString += ", [LastUpdate]"; $ValueString += ", '$(Get-Date)'"
                $Query = "Insert Into [$Database].[dbo].[$Table] ($ColumnString) Values ($ValueString)"
                Try {Query-SQL $Query}
                Catch {
                    "Failed to add new row for $Database`: $Table"
                    $Failed += $Row | Select @{N="Table";E={$Table}},@{N="Database";E={$Database}},@{N="SQLServer";E={$SQLServer}},*,@{N="Error";E={$Error[0].ToString()}}
                }
            }

            If ($Failed.Count -ne 0) {
                $FailLog = "$FailPath\$Table`_Failed_$(Get-Date -Format 'MM-dd-yy').csv"
                ########## >> If the log file already exists, increment the currently set file name with a number:
                $i = 2; While (Test-Path -Path $FailLog) {$FailLog = $FailLog -replace "(_\d*\.csv)|(\.csv)","_$i.csv"; $i++}
                $Failed | Export-Csv -Path $FailLog -Append -NoTypeInformation
            }


            ########## >> Write results to an external log:
            $EndTime = Get-Date ; $ProcessTime = "$([Math]::Round(($EndTime - $StartTime).TotalSeconds,2))"
            $NewRowCount = Get-RowCount -Table $Table
            $RecordsImported = $NewRowCount - $BeginRowCount
            $RecordsMissed = ($BeginRowCount + $RowsToImport) - $NewRowCount
            $SuccessRate = [Math]::Round(($NewRowCount - $BeginRowCount) / $RowsToImport * 100,2)
            [Array]$FileString = (($ImportFiles.Name -join "`n")).Trim()
            $Results += $Table | Select `
            @{N="DateTime";E={Get-Date -Format "MM/dd/yy HH:mm:ss"}},@{N="Database";E={$Database}},@{N="Table";E={$_}},
            @{N="RowsToImport";E={$RowsToImport}},@{N="BeginRowCount";E={$BeginRowCount}},@{N="NewRowCount";E={$NewRowCount}},@{N="RecordsImported";E={$RecordsImported}},@{N="RecordsMissed";E={$RecordsMissed}},
            @{N="SuccessRate";E={"$SuccessRate%"}},@{N="ProcessTime(Sec)";E={$ProcessTime}},@{N="FileCount";E={$ImportFiles.Count}},@{N="FilesImported";E={$FileString}}

            Write-LogEvent "Total records imported for $Database-$Table`: $RecordsImported/$RowsToImport ($SuccessRate%)"
            $Results[-1] | FT -AutoSize
            $Results[-1] | Export-Csv -Path $SQLImportLog -Append -NoTypeInformation
        }
    }

    [Double]$Records = 0; $Results.RecordsImported | ForEach {$Records += $_}
    If ($Records -eq 0) {Write-LogEvent "No new data to insert for database $Database"}
    Else {Write-LogEvent "Total records imported for database $Database`: $Records"}


    Remove-Item -Path $TempPath -Recurse -Force
    [System.GC]::Collect()

}
#endregion: ############################# >>> Parse/Format/Import Data Into SQL


#region: ############################# >>> Optional: Re-Import Data from the $FailLog

$FailLogs = Get-ChildItem -File -Path $FailPath | Where {$_.Name -match "$(Get-Date -Format 'MM-dd-yy')"}

<########## >> Use this section to attempt a re-import of rows captured in the $FailLogs:
If ($FailLogs.Count -ne 0) {$A = Read-Host "Press Enter when you're ready to attempt a re-import of the data captured in the failed log"}

ForEach ($Log in $FailLogs) {
    $StartTime = Get-Date; $Results = @()

    $Data = Import-Csv -Path $Log.FullName
    $DataSets = $Data | Group-Object SQLServer,Database,Table
    $RowsToImport = $Data.Count
    "Rows: $RowsToImport; DataSets: $($DataSets.Count.Count)"

    ForEach ($DataSet in $DataSets) {
        $SQLServer = ($DataSet.Name -split ", ")[0]; $Database = ($DataSet.Name -split ", ")[1]; $Table = ($DataSet.Name -split ", ")[2]
        $SqlConnection = New-Object System.Data.SqlClient.SqlConnection; $SqlCmd = New-Object System.Data.SqlClient.SqlCommand; $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
        $SqlConnection.ConnectionString = "Server = $SQLServer; Database = $Database; Integrated Security = True"; $SqlCmd.Connection = $SqlConnection; $SqlAdapter.SelectCommand = $SqlCmd

        $BeginRowCount = Get-RowCount -Table $Table

        $Data = $DataSet.Group | Select * -ExcludeProperty SQLServer,Database,Table,Error

        $Columns = ($Data[0] | ConvertTo-Csv -NoTypeInformation)[0].Replace('"',"").Split(",") #| Where {(Invoke-Expression "`$(`$DataSet.Name -split ', ')") -notcontains $_}
        ForEach ($Column in $Columns) {ForEach ($Row in $Data) {If ($Row."$Column" -eq '' -or $Row."$Column" -eq [DBNull]::Value) {$Row."$Column" = $null}}}

        $Failed = @()

        ForEach ($Row in $Data) {
                ##### >> Optional: Use the previously generated ReplaceString to replace incompatible characters:
                #ForEach ($Column in $Columns) {If ($Row."$Column" -match "$MatchString") {$Row."$Column" = Invoke-Expression "(`$Row.'$Column' $ReplaceString).Trim()"}}
            $ColumnString = ($Columns | Where {$Row."$_" -ne $null} | ForEach {"[$_]"}) -join ", "
            $ValueString = ($Columns | Where {$Row."$_" -ne $null} | ForEach {"'$($Row."$_")'"}) -join ", "
                ##### >> Optional: Insert a "LastUpdate" field into the imported SQL row:
                #$ColumnString += ", [LastUpdate]"; $ValueString += ", '$(Get-Date)'"
            $Query = "Insert Into [$Database].[dbo].[$Table] ($ColumnString) Values ($ValueString)"
            Try {Query-SQL $Query}
            Catch {
                "Failed to add new row for $Database`: $Table"
                $Failed += $Row | Select @{N="Table";E={$Table}},@{N="Database";E={$Database}},@{N="SQLServer";E={$SQLServer}},*,@{N="Error";E={$Error[0].ToString()}}
            }
        }

        $EndTime = Get-Date ; $ProcessTime = "$([Math]::Round(($EndTime - $StartTime).TotalSeconds,2))"
        $NewRowCount = Get-RowCount -Table $Table
        $RecordsImported = $NewRowCount - $BeginRowCount
        $RecordsMissed = ($BeginRowCount + $RowsToImport) - $NewRowCount
        $SuccessRate = [Math]::Round(($NewRowCount - $BeginRowCount) / $RowsToImport * 100,2)
        [Array]$FileString = (($Log.Name -join "`n")).Trim()
        $Results += $Table | Select `
        @{N="DateTime";E={Get-Date -Format "MM/dd/yy HH:mm:ss"}},@{N="Database";E={$Database}},@{N="Table";E={$_}},
        @{N="RowsToImport";E={$RowsToImport}},@{N="BeginRowCount";E={$BeginRowCount}},@{N="NewRowCount";E={$NewRowCount}},@{N="RecordsImported";E={$RecordsImported}},@{N="RecordsMissed";E={$RecordsMissed}},
        @{N="SuccessRate";E={"$SuccessRate%"}},@{N="ProcessTime(Sec)";E={$ProcessTime}},@{N="FileCount";E={$File.Count}},@{N="FilesImported";E={$FileString}}

        Write-LogEvent "Total records imported for $Database-$Table`: $RecordsImported/$RowsToImport ($SuccessRate%)"
        $Results[-1] | FT -AutoSize
        $Results[-1] | Export-Csv -Path $SQLImportLog -Append -NoTypeInformation

        If ($Failed.Count -eq 0) {Remove-Item -Path $Log.FullName}
        Else {$Failed | Export-Csv -Path $Log.FullName -NoTypeInformation}
    }
} #>

#endregion: ############################# >>> Re-Import Data from the $FailLog