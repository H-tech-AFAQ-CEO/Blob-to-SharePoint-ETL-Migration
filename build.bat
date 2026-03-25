@echo off
REM Build script for SharePoint ETL Pipeline (Windows)
REM Creates cross-platform executables for client deployment

echo Building SharePoint ETL Pipeline...

REM Clean previous builds
echo Cleaning previous builds...
if exist bin\Release rmdir /s /q bin\Release
dotnet clean

REM Build for Windows
echo Building for Windows (x64)...
dotnet publish -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true

REM Build for macOS (Intel)
echo Building for macOS (x64)...
dotnet publish -c Release -r osx-x64 --self-contained true -p:PublishSingleFile=true

REM Build for Linux
echo Building for Linux (x64)...
dotnet publish -c Release -r linux-x64 --self-contained true -p:PublishSingleFile=true

REM Create deployment packages
echo Creating deployment packages...

REM Windows package
copy bin\Release\net8.0\win-x64\publish\SharePointETL.exe SharePointETL-win-x64.exe
copy folder_mapping.csv folder_mapping.csv
copy README.md README.md
copy SETUP.md SETUP.md
powershell Compress-Archive -Path SharePointETL-win-x64.exe,folder_mapping.csv,README.md,SETUP.md -DestinationPath SharePointETL-Windows.zip

REM Mac package
copy bin\Release\net8.0\osx-x64\publish\SharePointETL SharePointETL-osx-x64
powershell Compress-Archive -Path SharePointETL-osx-x64,folder_mapping.csv,README.md,SETUP.md -DestinationPath SharePointETL-Mac.zip

REM Linux package
copy bin\Release\net8.0\linux-x64\publish\SharePointETL SharePointETL-linux-x64
powershell Compress-Archive -Path SharePointETL-linux-x64,folder_mapping.csv,README.md,SETUP.md -DestinationPath SharePointETL-Linux.zip

REM Cleanup
del SharePointETL-win-x64.exe SharePointETL-osx-x64 SharePointETL-linux-x64

echo Build complete! Packages created:
echo - SharePointETL-Windows.zip
echo - SharePointETL-Mac.zip
echo - SharePointETL-Linux.zip

pause
