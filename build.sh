#!/bin/bash

# Build script for SharePoint ETL Pipeline
# Creates cross-platform executables for client deployment

echo "Building SharePoint ETL Pipeline..."

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf bin/Release/
dotnet clean

# Build for Windows
echo "Building for Windows (x64)..."
dotnet publish -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true

# Build for macOS (Intel)
echo "Building for macOS (x64)..."
dotnet publish -c Release -r osx-x64 --self-contained true -p:PublishSingleFile=true

# Build for Linux
echo "Building for Linux (x64)..."
dotnet publish -c Release -r linux-x64 --self-contained true -p:PublishSingleFile=true

# Create deployment packages
echo "Creating deployment packages..."

# Windows package
cp bin/Release/net8.0/win-x64/publish/SharePointETL.exe ./SharePointETL-win-x64.exe
cp folder_mapping.csv ./folder_mapping.csv
cp README.md ./README.md
cp SETUP.md ./SETUP.md
zip -r SharePointETL-Windows.zip SharePointETL-win-x64.exe folder_mapping.csv README.md SETUP.md

# Mac package
cp bin/Release/net8.0/osx-x64/publish/SharePointETL ./SharePointETL-osx-x64
chmod +x SharePointETL-osx-x64
zip -r SharePointETL-Mac.zip SharePointETL-osx-x64 folder_mapping.csv README.md SETUP.md

# Linux package
cp bin/Release/net8.0/linux-x64/publish/SharePointETL ./SharePointETL-linux-x64
chmod +x SharePointETL-linux-x64
tar -czf SharePointETL-Linux.tar.gz SharePointETL-linux-x64 folder_mapping.csv README.md SETUP.md

# Cleanup
rm SharePointETL-win-x64.exe SharePointETL-osx-x64 SharePointETL-linux-x64

echo "Build complete! Packages created:"
echo "- SharePointETL-Windows.zip"
echo "- SharePointETL-Mac.zip"
echo "- SharePointETL-Linux.tar.gz"
