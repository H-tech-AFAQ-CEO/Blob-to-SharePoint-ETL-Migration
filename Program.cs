using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using System.Net.Http.Headers;
using System.Text.Json;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using System.Text;
using CsvHelper;
using System.Globalization;
using System.CommandLine;
using System.Security.Cryptography;
using System.Xml.Linq;
using Azure.Core;
using Azure.Storage;


namespace BlobToSharePointETL;

public class Program
{
    private static ILogger<Program>? _logger;

    public static async Task<int> Main(string[] args)
    {
        // Set up logging
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<Program>();

        try
        {
            var rootCommand = new RootCommand("Azure Blob to SharePoint ETL Pipeline");
            
            var dryRunOption = new Option<bool>("--dry-run", "Preview path mappings without importing");
            var testAuthOption = new Option<bool>("--test-auth", "Test SharePoint authentication only");
            var simulateOption = new Option<bool>("--simulate", "Simulate ultra-high performance migration with 1000 test files");
            
            rootCommand.AddOption(dryRunOption);
            rootCommand.AddOption(testAuthOption);
            rootCommand.AddOption(simulateOption);

            rootCommand.SetHandler(async (bool dryRun, bool testAuth, bool simulate) =>
            {
                if (simulate)
                {
                    await SimulateUltraHighPerformanceAsync();
                }
                else if (testAuth)
                {
                    await TestAuthenticationAsync();
                }
                else
                {
                    await RunPipelineAsync(dryRun);
                }
            }, dryRunOption, testAuthOption, simulateOption);

            return await rootCommand.InvokeAsync(args);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Application failed");
            return 1;
        }
    }

    private static async Task SimulateUltraHighPerformanceAsync()
    {
        _logger!.LogInformation("=========================================");
        _logger!.LogInformation("Ultra-High Performance Simulation (1000 Files)");
        _logger!.LogInformation("=========================================");

        var config = new Configuration
        {
            UseUltraHighPerformanceMode = true,
            BatchSize = 1000,
            MaxConcurrency = Environment.ProcessorCount * 50,
            MaxParallelBatches = 20,
            CopyPollingIntervalMs = 100,
            TargetFilesPerSecond = 28
        };

        var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        // Simulate 1000 files
        var files = Enumerable.Range(1, 1000).Select(i => new BlobFileInfo
        {
            SourcePath = $"test_folder/file_{i:D4}.pdf",
            FileName = $"file_{i:D4}.pdf",
            Extension = ".pdf",
            SizeBytes = 1024 * 1024, // 1MB each
            CreatedOn = DateTime.UtcNow.AddDays(-Random.Shared.Next(1, 365)),
            ModifiedOn = DateTime.UtcNow.AddDays(-Random.Shared.Next(1, 30)),
            Status = FileStatus.Pending,
            StagingStartTime = DateTime.UtcNow
        }).ToList();

        _logger!.LogInformation($"Created {files.Count:N0} test files for simulation");

        // Stage 1: Ultra-High Performance Staging Simulation
        _logger!.LogInformation("[Stage 1/2] Ultra-High Performance Staging Simulation...");
        var stagingStopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        var batchSize = config.BatchSize;
        var maxConcurrency = config.MaxConcurrency;
        var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        
        var stagingTasks = files.Select(async (file, index) =>
        {
            await semaphore.WaitAsync();
            try
            {
                var fileStopwatch = System.Diagnostics.Stopwatch.StartNew();
                
                // Simulate staging time (50-150ms)
                var stagingTime = Random.Shared.Next(50, 150);
                await Task.Delay(stagingTime);
                
                fileStopwatch.Stop();
                file.StagingEndTime = DateTime.UtcNow;
                file.Status = FileStatus.Staged;
                file.PackageId = "1";
                file.StagedBlobPath = $"1/content/{file.TargetPath}";
                
                return file;
            }
            finally
            {
                semaphore.Release();
            }
        });

        var stagedFiles = await Task.WhenAll(stagingTasks);
        stagingStopwatch.Stop();
        
        var stagingSpeed = stagedFiles.Length / (stagingStopwatch.ElapsedMilliseconds / 1000.0);
        _logger!.LogInformation($"Staging Complete: {stagedFiles.Length:N0} files in {stagingStopwatch.ElapsedMilliseconds / 1000.0:F1}s");
        _logger!.LogInformation($"Staging Speed: {stagingSpeed:F1} files/second");

        // Stage 2: Ultra-High Performance Migration Simulation
        _logger!.LogInformation("[Stage 2/2] Ultra-High Performance Migration Simulation...");
        var migrationStopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        // Group into batches for migration
        var batches = stagedFiles.GroupBy(f => f.PackageId).ToList();
        var migrationTasks = batches.Select(async batch =>
        {
            var batchFiles = batch.ToList();
            var batchStopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            // Simulate migration time (5-15 seconds per batch)
            var migrationTime = Random.Shared.Next(5000, 15000);
            await Task.Delay(migrationTime);
            
            batchStopwatch.Stop();
            
            foreach (var file in batchFiles)
            {
                file.MigrationStartTime = DateTime.UtcNow;
                file.MigrationEndTime = DateTime.UtcNow;
                file.Status = FileStatus.Migrated;
                file.SharePointUrl = $"https://sharepoint.com/Documents/{file.TargetPath}";
            }
            
            return batchFiles.Count;
        });

        var migrationResults = await Task.WhenAll(migrationTasks);
        migrationStopwatch.Stop();
        
        totalStopwatch.Stop();
        
        var totalMigrated = migrationResults.Sum();
        var totalSpeed = totalMigrated / (totalStopwatch.ElapsedMilliseconds / 1000.0);
        var efficiency = (totalSpeed / config.TargetFilesPerSecond) * 100;
        
        _logger!.LogInformation("=========================================");
        _logger!.LogInformation("Ultra-High Performance Simulation Results:");
        _logger!.LogInformation("=========================================");
        _logger!.LogInformation($"Total Files: {totalMigrated:N0}");
        _logger!.LogInformation($"Total Time: {totalStopwatch.ElapsedMilliseconds / 1000.0:F1}s");
        _logger!.LogInformation($"Overall Speed: {totalSpeed:F1} files/second");
        _logger!.LogInformation($"Target Speed: {config.TargetFilesPerSecond} files/second");
        _logger!.LogInformation($"Efficiency: {efficiency:F1}%");
        _logger!.LogInformation($"Staging Speed: {stagingSpeed:F1} files/second");
        _logger!.LogInformation($"Concurrency: {maxConcurrency}");
        _logger!.LogInformation($"Batch Size: {batchSize}");
        _logger!.LogInformation("=========================================");
        
        // Calculate extrapolated performance for 100K and 1M files
        var timeFor100K = (100000 / totalSpeed) / 3600.0; // hours
        var timeFor1M = (1000000 / totalSpeed) / 3600.0; // hours
        
        _logger!.LogInformation("Extrapolated Performance:");
        _logger!.LogInformation($"100K files: {timeFor100K:F1} hours");
        _logger!.LogInformation($"1M files: {timeFor1M:F1} hours");
        _logger!.LogInformation("=========================================");
    }

    private static async Task TestAuthenticationAsync()
    {
        _logger!.LogInformation("=========================================");
        _logger!.LogInformation("Testing SharePoint Authentication");
        _logger!.LogInformation("=========================================");

        var config = LoadConfiguration();
        
        if (string.IsNullOrEmpty(config.TenantId) || string.IsNullOrEmpty(config.ClientId) || 
            string.IsNullOrEmpty(config.ClientSecret) || string.IsNullOrEmpty(config.SiteUrl))
        {
            _logger!.LogError("Missing required configuration. Please set:");
            _logger!.LogError("- SP_TENANT_ID");
            _logger!.LogError("- SP_CLIENT_ID");
            _logger!.LogError("- SP_CLIENT_SECRET");
            _logger!.LogError("- SP_SITE_URL");
            return;
        }

        try
        {
            // Get access token
            var app = ConfidentialClientApplicationBuilder
                .Create(config.ClientId)
                .WithClientSecret(config.ClientSecret)
                .WithAuthority(new Uri($"https://login.microsoftonline.com/{config.TenantId}"))
                .Build();

            var scopes = new[] { "https://graph.microsoft.com/.default" };
            var result = await app.AcquireTokenForClient(scopes).ExecuteAsync();

            _logger!.LogInformation("✓ Access token acquired successfully");

            // Test site resolution
            using var httpClient = new HttpClient();
            var baseUrl = "https://graph.microsoft.com/v1.0";
            
            var uri = new Uri(config.SiteUrl);
            var hostname = uri.Host;
            var serverRelativePath = uri.AbsolutePath.Trim('/');

            var siteRequestUrl = $"{baseUrl}/sites/{hostname}:/{serverRelativePath}";
            
            var request = new HttpRequestMessage(HttpMethod.Get, siteRequestUrl);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", result.AccessToken);

            var response = await httpClient.SendAsync(request);
            var responseContent = await response.Content.ReadAsStringAsync();

            if (response.IsSuccessStatusCode)
            {
                var siteResult = JsonSerializer.Deserialize<JsonElement>(responseContent);
                var siteId = siteResult.GetProperty("id").GetString();
                var siteName = siteResult.GetProperty("displayName").GetString();
                
                _logger!.LogInformation($"✓ Site resolved: {siteName}");
                _logger!.LogInformation($"✓ Site ID: {siteId}");
                _logger!.LogInformation("✓ Authentication test passed!");
            }
            else
            {
                _logger!.LogError($"✗ Site resolution failed: {response.StatusCode}");
                _logger!.LogError($"Response: {responseContent}");
            }
        }
        catch (Exception ex)
        {
            _logger!.LogError(ex, "Authentication test failed");
        }
    }

    private static async Task RunPipelineAsync(bool dryRun)
    {
        _logger!.LogInformation("=========================================");
        _logger!.LogInformation("Azure Blob -> SharePoint ETL Pipeline");
        _logger!.LogInformation("=========================================");

        var config = LoadConfiguration();

        // Validate configuration
        if (string.IsNullOrEmpty(config.StorageConnectionString) || string.IsNullOrEmpty(config.SourceContainerName))
        {
            _logger!.LogError("Missing Azure Storage configuration");
            return;
        }

        try
        {
            // Stage 1: Inventory
            _logger!.LogInformation("[Stage 1/4] Starting blob inventory...");
            var files = await InventoryBlobsAsync(config);
            _logger!.LogInformation($"Found {files.Count} supported files");

            // Stage 2: Transform paths
            _logger!.LogInformation("[Stage 2/4] Transforming paths...");
            var mappingRules = LoadMappingRules();
            TransformPaths(files, mappingRules);

            var excluded = files.Count(f => f.Status == FileStatus.Excluded);
            _logger!.LogInformation($"Transformed {files.Count - excluded} files, excluded {excluded}");

            if (dryRun)
            {
                _logger!.LogInformation("=== DRY RUN: Path Mapping Preview ===");
                foreach (var file in files.Where(f => f.Status == FileStatus.Pending).Take(20))
                {
                    _logger!.LogInformation($"  {file.SourcePath} -> {file.TargetPath}");
                }
                _logger!.LogInformation($"Total: {files.Count} files, {excluded} excluded");
                return;
            }

            // Stage 3: Stage files (simplified version)
            _logger!.LogInformation("[Stage 3/4] Staging files...");
            await StageFilesAsync(files, config);

            // Stage 4: Import to SharePoint
            _logger!.LogInformation("[Stage 4/4] Starting migration...");
            await MigrateToSharePointAsync(files, config);

            _logger!.LogInformation("Pipeline completed successfully!");
        }
        catch (Exception ex)
        {
            _logger!.LogError(ex, "Pipeline failed");
            throw;
        }
    }

    private static Configuration LoadConfiguration()
    {
        return new Configuration
        {
            StorageConnectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING") ?? "",
            SourceContainerName = Environment.GetEnvironmentVariable("BLOB_CONTAINER_NAME") ?? "",
            StagingContainerName = Environment.GetEnvironmentVariable("STAGING_CONTAINER_NAME") ?? "spmt-staging",
            TenantId = Environment.GetEnvironmentVariable("SP_TENANT_ID") ?? "",
            ClientId = Environment.GetEnvironmentVariable("SP_CLIENT_ID") ?? "",
            ClientSecret = Environment.GetEnvironmentVariable("SP_CLIENT_SECRET") ?? "",
            SiteUrl = Environment.GetEnvironmentVariable("SP_SITE_URL") ?? "",
            DocLibrary = Environment.GetEnvironmentVariable("SP_DOC_LIBRARY") ?? "Documents"
        };
    }

    private static async Task<List<BlobFileInfo>> InventoryBlobsAsync(Configuration config)
    {
        var files = new List<BlobFileInfo>();
        var supportedExtensions = new HashSet<string> { ".pdf", ".csv", ".html", ".htm" };

        var blobServiceClient = new BlobServiceClient(config.StorageConnectionString);
        var containerClient = blobServiceClient.GetBlobContainerClient(config.SourceContainerName);

        var fileCount = 0;
        await foreach (var blobItem in containerClient.GetBlobsAsync())
        {
            if (fileCount >= 2) // Limit to 2 files for PoC
                break;
                
            var extension = Path.GetExtension(blobItem.Name).ToLowerInvariant();
            if (supportedExtensions.Contains(extension))
            {
                var blobClient = containerClient.GetBlobClient(blobItem.Name);
                var properties = await blobClient.GetPropertiesAsync();

                files.Add(new BlobFileInfo
                {
                    SourcePath = blobItem.Name,
                    FileName = Path.GetFileName(blobItem.Name),
                    Extension = extension,
                    SizeBytes = properties.Value.ContentLength,
                    CreatedOn = properties.Value.CreatedOn.UtcDateTime,
                    ModifiedOn = properties.Value.LastModified.UtcDateTime,
                    Status = FileStatus.Pending
                });
                
                fileCount++;
            }
        }

        _logger!.LogInformation($"PoC: Limited to {files.Count} files for testing");
        return files;
    }

    private static List<MappingRule> LoadMappingRules()
    {
        var rules = new List<MappingRule>();
        var mappingFile = "folder_mapping.csv";

        if (!File.Exists(mappingFile))
        {
            _logger!.LogWarning($"Mapping file {mappingFile} not found. Using verbatim paths.");
            return rules;
        }

        using var reader = new StreamReader(mappingFile);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        
        csv.Read();
        csv.ReadHeader();
        
        while (csv.Read())
        {
            var source = csv.GetField(0)?.Trim().Trim('/');
            var target = csv.GetField(1)?.Trim().Trim('/');
            
            if (!string.IsNullOrEmpty(source))
            {
                rules.Add(new MappingRule
                {
                    SourcePrefix = source,
                    TargetPrefix = string.IsNullOrEmpty(target) ? null : target
                });
            }
        }

        // Sort by length (longest first) for proper precedence
        rules.Sort((a, b) => b.SourcePrefix.Length.CompareTo(a.SourcePrefix.Length));
        
        _logger!.LogInformation($"Loaded {rules.Count} mapping rules from {mappingFile}");
        return rules;
    }

    private static void TransformPaths(List<BlobFileInfo> files, List<MappingRule> rules)
    {
        foreach (var file in files)
        {
            var sourcePath = file.SourcePath.Trim('/');
            var folder = Path.GetDirectoryName(sourcePath)?.Replace('\\', '/') ?? "";
            var fileName = Path.GetFileName(sourcePath);

            var matched = false;
            foreach (var rule in rules)
            {
                if (folder == rule.SourcePrefix || folder.StartsWith(rule.SourcePrefix + "/"))
                {
                    if (rule.TargetPrefix == null)
                    {
                        file.Status = FileStatus.Excluded;
                        file.TargetPath = null;
                    }
                    else
                    {
                        var remainder = folder.Length > rule.SourcePrefix.Length 
                            ? folder.Substring(rule.SourcePrefix.Length + 1)
                            : "";
                        var newFolder = string.IsNullOrEmpty(remainder) 
                            ? rule.TargetPrefix 
                            : $"{rule.TargetPrefix}/{remainder}";
                        file.TargetPath = $"{newFolder}/{fileName}";
                    }
                    matched = true;
                    break;
                }
            }

            if (!matched)
            {
                // No rule matched, use verbatim path
                file.TargetPath = sourcePath;
            }
        }
    }

    private static async Task StageFilesAsync(List<BlobFileInfo> files, Configuration config)
    {
        _logger!.LogInformation("[Stage 3/4] Starting ultra-high performance staging...");
        
        var blobServiceClient = new BlobServiceClient(config.StorageConnectionString);
        var stagingContainer = blobServiceClient.GetBlobContainerClient(config.StagingContainerName);
        
        // Create staging container if it doesn't exist
        await stagingContainer.CreateIfNotExistsAsync();
        
        var stagedFiles = new List<BlobFileInfo>();
        var batchSize = config.UseUltraHighPerformanceMode ? config.BatchSize : 2;
        var maxParallelBatches = config.UseUltraHighPerformanceMode ? config.MaxParallelBatches : 1;
        
        _logger!.LogInformation($"Ultra-High Performance Mode: {batchSize:N0} files/batch, {maxParallelBatches} parallel batches");
        
        // Create batch packages
        var batches = new List<List<BlobFileInfo>>();
        var currentBatch = new List<BlobFileInfo>();
        var packageCounter = 1;
        
        foreach (var file in files.Where(f => f.Status == FileStatus.Pending))
        {
            currentBatch.Add(file);
            if (currentBatch.Count >= batchSize)
            {
                batches.Add(new List<BlobFileInfo>(currentBatch));
                currentBatch.Clear();
            }
        }
        
        if (currentBatch.Any())
            batches.Add(currentBatch);
        
        _logger!.LogInformation($"Created {batches.Count:N0} batches for {files.Count:N0} files");
        
        // Process batches in parallel with controlled concurrency
        var batchSemaphore = new SemaphoreSlim(maxParallelBatches, maxParallelBatches);
        var batchTasks = batches.Select(async (batch, index) =>
        {
            await batchSemaphore.WaitAsync();
            try
            {
                var packageId = (index + 1).ToString();
                await ProcessBatchUltraHighPerformanceAsync(batch, packageId, blobServiceClient, config);
                return batch.Count;
            }
            finally
            {
                batchSemaphore.Release();
            }
        });
        
        var results = await Task.WhenAll(batchTasks);
        stagedFiles.AddRange(files.Where(f => f.Status == FileStatus.Staged));
        
        _logger!.LogInformation($"Ultra-High Performance Staging Complete: {stagedFiles.Count:N0} files staged in {batches.Count:N0} batches");
    }

    private static async Task ProcessBatchUltraHighPerformanceAsync(List<BlobFileInfo> batch, string packageId, BlobServiceClient blobServiceClient, Configuration config)
    {
        var packagePath = $"{packageId}/content";
        var stagingContainer = blobServiceClient.GetBlobContainerClient(config.StagingContainerName);
        
        _logger!.LogInformation($"Ultra-High Performance Batch {packageId}: {batch.Count:N0} files...");
        
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        // Ultra-high performance parallelization
        var maxConcurrency = config.UseUltraHighPerformanceMode ? config.MaxConcurrency : Math.Min(batch.Count, Environment.ProcessorCount * 2);
        var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        
        // Pre-create all blob clients to avoid repeated overhead
        var sourceContainer = blobServiceClient.GetBlobContainerClient(config.SourceContainerName);
        var copyOperations = batch.Select(async file =>
        {
            await semaphore.WaitAsync();
            try
            {
                var fileStopwatch = System.Diagnostics.Stopwatch.StartNew();
                
                var sourceBlob = sourceContainer.GetBlobClient(file.SourcePath);
                var stagingBlob = stagingContainer.GetBlobClient($"{packagePath}/{file.TargetPath}");
                
                // Start copy operation immediately (no delays)
                await stagingBlob.StartCopyFromUriAsync(sourceBlob.Uri);
                
                // Optimized polling with configurable interval
                var copyStatus = CopyStatus.Pending;
                var attempts = 0;
                var maxAttempts = 60; // Reduced max attempts for faster processing
                var pollingInterval = config.CopyPollingIntervalMs;
                
                while (copyStatus == CopyStatus.Pending && attempts < maxAttempts)
                {
                    await Task.Delay(pollingInterval);
                    var properties = await stagingBlob.GetPropertiesAsync();
                    copyStatus = properties.Value.CopyStatus;
                    attempts++;
                    
                    // Reduced logging for performance
                    if (attempts % 20 == 0) // Log every 20 attempts vs 30
                    {
                        _logger!.LogDebug($"Still copying {file.FileName} - attempt {attempts}/{maxAttempts}");
                    }
                }
                
                fileStopwatch.Stop();
                
                if (copyStatus != CopyStatus.Success)
                {
                    throw new Exception($"Copy failed with status: {copyStatus} after {attempts} attempts ({fileStopwatch.ElapsedMilliseconds}ms)");
                }
                
                // Update file info
                file.PackageId = packageId;
                file.StagedBlobPath = $"{packagePath}/{file.TargetPath}";
                file.Status = FileStatus.Staged;
                
                return file;
            }
            catch (Exception ex)
            {
                _logger!.LogError(ex, $"Failed to stage file: {file.FileName}");
                file.Status = FileStatus.Error;
                file.ErrorMessage = ex.Message;
                return file;
            }
            finally
            {
                semaphore.Release();
            }
        });
        
        var results = await Task.WhenAll(copyOperations);
        stopwatch.Stop();
        
        var successCount = results.Count(f => f.Status == FileStatus.Staged);
        var errorCount = results.Count(f => f.Status == FileStatus.Error);
        var filesPerSecond = batch.Count / (stopwatch.ElapsedMilliseconds / 1000.0);
        
        _logger!.LogInformation($"Ultra-High Performance Batch {packageId} Complete:");
        _logger!.LogInformation($"  - Files: {successCount:N0} succeeded, {errorCount:N0} failed");
        _logger!.LogInformation($"  - Time: {stopwatch.ElapsedMilliseconds / 1000.0:F1}s");
        _logger!.LogInformation($"  - Speed: {filesPerSecond:F1} files/second");
        _logger!.LogInformation($"  - Concurrency: {maxConcurrency}");
    }

    private static async Task MigrateToSharePointAsync(List<BlobFileInfo> files, Configuration config)
    {
        _logger!.LogInformation("[Stage 4/4] Starting Ultra-High Performance SharePoint Migration...");
        
        var token = await GetAccessTokenAsync(config);
        var siteId = await GetSiteIdAsync(config, token);
        
        // Group staged files by package ID for batch processing
        var packageGroups = files
            .Where(f => f.Status == FileStatus.Staged)
            .GroupBy(f => f.PackageId)
            .ToList();
        
        _logger!.LogInformation($"Ultra-High Performance: Processing {packageGroups.Count:N0} migration packages...");
        
        var migrationStopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        // Ultra-high performance parallel migration jobs
        var maxParallelJobs = config.UseUltraHighPerformanceMode ? config.MaxParallelBatches : 1;
        var jobSemaphore = new SemaphoreSlim(maxParallelJobs, maxParallelJobs);
        
        var migrationTasks = packageGroups.Select(async packageGroup =>
        {
            await jobSemaphore.WaitAsync();
            try
            {
                var packageId = packageGroup.Key;
                var packageFiles = packageGroup.ToList();
                
                _logger!.LogInformation($"Ultra-High Performance Migration: Package {packageId} - {packageFiles.Count:N0} files");
                
                var packageStopwatch = System.Diagnostics.Stopwatch.StartNew();
                
                // Generate migration manifest
                var manifest = await GenerateMigrationManifestAsync(packageFiles, config);
                var manifestBlobPath = await UploadManifestAsync(manifest, packageId, config);
                
                // Generate SAS URL for staging container
                var sasUrl = await GenerateContainerSasUrlAsync(config);
                
                // Submit migration job
                var jobId = await SubmitMigrationJobAsync(packageId, manifestBlobPath, sasUrl, siteId, token, config);
                
                // Poll for job completion with ultra-high performance settings
                await PollMigrationJobUltraHighPerformanceAsync(jobId, siteId, token, packageFiles, config);
                
                packageStopwatch.Stop();
                var filesPerSecond = packageFiles.Count / (packageStopwatch.ElapsedMilliseconds / 1000.0);
                
                _logger!.LogInformation($"Ultra-High Performance Package {packageId} Complete:");
                _logger!.LogInformation($"  - Files: {packageFiles.Count:N0}");
                _logger!.LogInformation($"  - Time: {packageStopwatch.ElapsedMilliseconds / 1000.0:F1}s");
                _logger!.LogInformation($"  - Speed: {filesPerSecond:F1} files/second");
                
                return packageFiles.Count;
            }
            finally
            {
                jobSemaphore.Release();
            }
        });
        
        var results = await Task.WhenAll(migrationTasks);
        migrationStopwatch.Stop();
        
        var totalMigrated = results.Sum();
        var overallSpeed = totalMigrated / (migrationStopwatch.ElapsedMilliseconds / 1000.0);
        
        _logger!.LogInformation($"Ultra-High Performance Migration Complete:");
        _logger!.LogInformation($"  - Total Files: {totalMigrated:N0}");
        _logger!.LogInformation($"  - Total Time: {migrationStopwatch.ElapsedMilliseconds / 1000.0 / 3600.0:F1} hours");
        _logger!.LogInformation($"  - Overall Speed: {overallSpeed:F1} files/second");
        _logger!.LogInformation($"  - Target Speed: {config.TargetFilesPerSecond} files/second");
        _logger!.LogInformation($"  - Efficiency: {(overallSpeed / config.TargetFilesPerSecond * 100):F1}%");
        
        // Generate final report
        await GenerateMigrationReportAsync(files, config);
    }

    private static async Task<string> GenerateMigrationManifestAsync(List<BlobFileInfo> files, Configuration config)
    {
        _logger!.LogDebug($"Generating migration manifest for {files.Count} files...");
        
        var manifest = new
        {
            Version = "15.0.0.0",
            MigrationType = "FileMigration",
            ManifestEntries = files.Select(file => new
            {
                SourcePath = file.StagedBlobPath,
                TargetPath = $"{config.DocLibrary}/{file.TargetPath}",
                FileSize = file.SizeBytes,
                Created = file.CreatedOn.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                Modified = file.ModifiedOn.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                FileHash = ComputeFileHash(file),
                ContentType = GetContentType(file.Extension)
            }).ToArray()
        };
        
        var manifestJson = JsonSerializer.Serialize(manifest, new JsonSerializerOptions { WriteIndented = true });
        _logger!.LogDebug($"Generated manifest: {manifestJson.Length} characters");
        
        return manifestJson;
    }

    private static async Task<string> UploadManifestAsync(string manifestJson, string packageId, Configuration config)
    {
        var blobServiceClient = new BlobServiceClient(config.StorageConnectionString);
        var stagingContainer = blobServiceClient.GetBlobContainerClient(config.StagingContainerName);
        
        var manifestBlobPath = $"{packageId}/manifest.json";
        var manifestBlob = stagingContainer.GetBlobClient(manifestBlobPath);
        
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(manifestJson));
        await manifestBlob.UploadAsync(stream);
        
        _logger!.LogDebug($"Manifest uploaded to: {manifestBlobPath}");
        return manifestBlobPath;
    }

    private static async Task<string> GenerateContainerSasUrlAsync(Configuration config)
    {
        var blobServiceClient = new BlobServiceClient(config.StorageConnectionString);
        var stagingContainer = blobServiceClient.GetBlobContainerClient(config.StagingContainerName);
        
        // Generate SAS token valid for 48 hours for long-running migrations
        var sasBuilder = new BlobSasBuilder
        {
            BlobContainerName = config.StagingContainerName,
            Resource = "c",
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-15), // Start 15 minutes ago for clock skew
            ExpiresOn = DateTimeOffset.UtcNow.AddHours(48),
            Protocol = SasProtocol.Https
        };
        
        sasBuilder.SetPermissions(BlobContainerSasPermissions.Read | BlobContainerSasPermissions.List);
        
        // Extract account key from connection string
        var connectionStringParts = config.StorageConnectionString.Split(';');
        var accountKey = "";
        var accountName = "";
        
        foreach (var part in connectionStringParts)
        {
            if (part.StartsWith("AccountKey=", StringComparison.OrdinalIgnoreCase))
                accountKey = part.Substring("AccountKey=".Length);
            else if (part.StartsWith("AccountName=", StringComparison.OrdinalIgnoreCase))
                accountName = part.Substring("AccountName=".Length);
        }
        
        if (string.IsNullOrEmpty(accountKey) || string.IsNullOrEmpty(accountName))
            throw new Exception("Could not extract AccountName or AccountKey from connection string");
        
        var sasToken = sasBuilder.ToSasQueryParameters(new StorageSharedKeyCredential(
            accountName, accountKey)).ToString();
        
        var sasUrl = $"{stagingContainer.Uri}?{sasToken}";
        _logger!.LogDebug($"Generated SAS URL for container: {config.StagingContainerName} (valid 48 hours)");
        
        return sasUrl;
    }

    private static async Task<string> SubmitMigrationJobAsync(string packageId, string manifestPath, string sasUrl, string siteId, string token, Configuration config)
    {
        using var httpClient = new HttpClient();
        
        var migrationRequest = new
        {
            MigrationType = "FileMigration",
            SourceDataLocation = new
            {
                FileContainerUrl = sasUrl,
                ManifestFileUrl = $"{sasUrl.Replace("?", $"/{manifestPath}?")}"
            },
            Destination = new
            {
                SiteId = siteId,
                LibraryName = config.DocLibrary
            },
            Preferences = new
            {
                PreserveTimestamps = true,
                PreservePermissions = false
            }
        };
        
        var jsonContent = JsonSerializer.Serialize(migrationRequest);
        var request = new HttpRequestMessage(HttpMethod.Post, $"https://graph.microsoft.com/v1.0/sites/{siteId}/migrate")
        {
            Content = new StringContent(jsonContent, Encoding.UTF8, "application/json")
        };
        
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        
        var response = await httpClient.SendAsync(request);
        var responseContent = await response.Content.ReadAsStringAsync();
        
        if (!response.IsSuccessStatusCode)
        {
            throw new Exception($"Migration job submission failed: {response.StatusCode} - {responseContent}");
        }
        
        var result = JsonSerializer.Deserialize<JsonElement>(responseContent);
        var jobId = result.GetProperty("id").GetString();
        
        _logger!.LogInformation($"Migration job submitted: {jobId}");
        return jobId!;
    }

    private static async Task PollMigrationJobUltraHighPerformanceAsync(string jobId, string siteId, string token, List<BlobFileInfo> files, Configuration config)
    {
        using var httpClient = new HttpClient();
        var maxAttempts = config.UseUltraHighPerformanceMode ? 720 : 60; // 6 hours vs 30 minutes
        var pollingInterval = config.UseUltraHighPerformanceMode ? 10 : 30; // 10s vs 30s intervals
        var attempt = 0;
        
        _logger!.LogInformation($"Ultra-High Performance Polling: Job {jobId} (max {maxAttempts} attempts, {pollingInterval}s intervals)");
        
        var jobStopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        while (attempt < maxAttempts)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(pollingInterval));
                attempt++;
                
                var request = new HttpRequestMessage(HttpMethod.Get, $"https://graph.microsoft.com/v1.0/sites/{siteId}/migrate/{jobId}");
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                
                var response = await httpClient.SendAsync(request);
                var responseContent = await response.Content.ReadAsStringAsync();
                
                if (response.IsSuccessStatusCode)
                {
                    var result = JsonSerializer.Deserialize<JsonElement>(responseContent);
                    var status = result.GetProperty("status").GetString();
                    
                    // Reduced logging frequency for performance
                    if (attempt % 10 == 0 || status is "completed" or "failed")
                    {
                        _logger!.LogInformation($"Ultra-High Performance Job {jobId}: {status} (Attempt {attempt}/{maxAttempts})");
                    }
                    
                    switch (status)
                    {
                        case "completed":
                            jobStopwatch.Stop();
                            var completedFiles = result.TryGetProperty("completedItems", out var completedProp) 
                                ? completedProp.GetInt32() 
                                : files.Count;
                            var filesPerSecond = completedFiles / (jobStopwatch.ElapsedMilliseconds / 1000.0);
                            
                            _logger!.LogInformation($"Ultra-High Performance Job {jobId} COMPLETED:");
                            _logger!.LogInformation($"  - Files: {completedFiles:N0}");
                            _logger!.LogInformation($"  - Time: {jobStopwatch.ElapsedMilliseconds / 1000.0:F1}s");
                            _logger!.LogInformation($"  - Speed: {filesPerSecond:F1} files/second");
                            
                            foreach (var file in files)
                            {
                                file.Status = FileStatus.Migrated;
                                file.SharePointUrl = $"{config.DocLibrary}/{file.TargetPath}";
                            }
                            return;
                            
                        case "failed":
                            jobStopwatch.Stop();
                            var error = result.GetProperty("error").GetString();
                            var failedItems = result.TryGetProperty("failedItems", out var failedProp) 
                                ? failedProp.GetInt32() 
                                : 0;
                            
                            _logger!.LogError($"Ultra-High Performance Job {jobId} FAILED:");
                            _logger!.LogError($"  - Error: {error}");
                            _logger!.LogError($"  - Failed Items: {failedItems:N0}");
                            _logger!.LogError($"  - Time: {jobStopwatch.ElapsedMilliseconds / 1000.0:F1}s");
                            
                            throw new Exception($"Migration job failed: {error}");
                            
                        case "inProgress":
                            var progress = result.TryGetProperty("progress", out var progressProp) 
                                ? progressProp.GetInt32() 
                                : 0;
                            
                            // Log progress every 50 attempts for performance
                            if (attempt % 50 == 0)
                            {
                                _logger!.LogDebug($"Ultra-High Performance Job {jobId}: {progress}% complete");
                            }
                            continue;
                            
                        default:
                            _logger!.LogWarning($"Ultra-High Performance Job {jobId}: Unknown status {status}");
                            continue;
                    }
                }
                else
                {
                    _logger!.LogWarning($"Ultra-High Performance Job {jobId}: HTTP {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                _logger!.LogError(ex, $"Ultra-High Performance Job {jobId}: Error on attempt {attempt}/{maxAttempts}");
                if (attempt >= maxAttempts)
                    throw;
                
                // Faster retry on transient errors
                if (ex is HttpRequestException || ex is TaskCanceledException)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5)); // Quick retry
                    continue;
                }
                else
                {
                    throw;
                }
            }
        }
        
        jobStopwatch.Stop();
        throw new TimeoutException($"Ultra-High Performance Job {jobId} did not complete within {maxAttempts * pollingInterval} seconds ({maxAttempts * pollingInterval / 3600.0:F1} hours)");
    }

    private static string ComputeFileHash(BlobFileInfo file)
    {
        // For now, return a placeholder - in production, compute actual MD5 hash
        return Guid.NewGuid().ToString("N");
    }

    private static string GetContentType(string extension)
    {
        return extension.ToLowerInvariant() switch
        {
            ".pdf" => "application/pdf",
            ".csv" => "text/csv",
            ".html" or ".htm" => "text/html",
            _ => "application/octet-stream"
        };
    }

    private static async Task GenerateMigrationReportAsync(List<BlobFileInfo> files, Configuration config)
    {
        var reportPath = $"migration_report_{DateTime.UtcNow:yyyyMMdd_HHmmss}.json";
        var report = new
        {
            GeneratedAt = DateTime.UtcNow,
            Configuration = new
            {
                SourceContainer = config.SourceContainerName,
                StagingContainer = config.StagingContainerName,
                SiteUrl = config.SiteUrl,
                DocLibrary = config.DocLibrary
            },
            Summary = new
            {
                TotalFiles = files.Count,
                Migrated = files.Count(f => f.Status == FileStatus.Migrated),
                Failed = files.Count(f => f.Status == FileStatus.Error),
                Excluded = files.Count(f => f.Status == FileStatus.Excluded),
                TotalSizeBytes = files.Where(f => f.Status != FileStatus.Excluded).Sum(f => f.SizeBytes)
            },
            Files = files.Select(f => new
            {
                f.SourcePath,
                f.TargetPath,
                f.Status,
                f.SharePointUrl,
                f.ErrorMessage,
                f.PackageId,
                FileSize = f.SizeBytes
            })
        };

        await File.WriteAllTextAsync(reportPath, JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));
        _logger!.LogInformation($"Migration report saved to: {reportPath}");
        
        // Log summary
        _logger!.LogInformation("=========================================");
        _logger!.LogInformation("Migration Summary:");
        _logger!.LogInformation($"Total files: {files.Count}");
        _logger!.LogInformation($"Migrated: {files.Count(f => f.Status == FileStatus.Migrated)}");
        _logger!.LogInformation($"Failed: {files.Count(f => f.Status == FileStatus.Error)}");
        _logger!.LogInformation($"Excluded: {files.Count(f => f.Status == FileStatus.Excluded)}");
        _logger!.LogInformation("=========================================");
    }

    private static async Task<string> GetAccessTokenAsync(Configuration config)
    {
        // Client credentials OAuth flow with MSAL for high-performance authentication
        var app = ConfidentialClientApplicationBuilder
            .Create(config.ClientId)
            .WithClientSecret(config.ClientSecret)
            .WithAuthority(new Uri($"https://login.microsoftonline.com/{config.TenantId}"))
            .Build();

        var scopes = new[] { "https://graph.microsoft.com/.default" };
        var result = await app.AcquireTokenForClient(scopes).ExecuteAsync();
        return result.AccessToken;
    }

    private static async Task<string> GetSiteIdAsync(Configuration config, string token)
    {
        using var httpClient = new HttpClient();
        var baseUrl = "https://graph.microsoft.com/v1.0";
        
        var uri = new Uri(config.SiteUrl);
        var hostname = uri.Host;
        var serverRelativePath = uri.AbsolutePath.Trim('/');

        var siteRequestUrl = $"{baseUrl}/sites/{hostname}:/{serverRelativePath}";
        
        var request = new HttpRequestMessage(HttpMethod.Get, siteRequestUrl);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        var response = await httpClient.SendAsync(request);
        var responseContent = await response.Content.ReadAsStringAsync();

        if (response.IsSuccessStatusCode)
        {
            var siteResult = JsonSerializer.Deserialize<JsonElement>(responseContent);
            return siteResult.GetProperty("id").GetString()!;
        }
        
        throw new Exception($"Failed to resolve site ID: {response.StatusCode} - {responseContent}");
    }
}

public class Configuration
{
    public string StorageConnectionString { get; set; } = "";
    public string SourceContainerName { get; set; } = "";
    public string StagingContainerName { get; set; } = "";
    public string TenantId { get; set; } = "";
    public string ClientId { get; set; } = "";
    public string ClientSecret { get; set; } = "";
    public string SiteUrl { get; set; } = "";
    public string DocLibrary { get; set; } = "";
    
    // Ultra-high performance configuration for 1M files
    public int BatchSize { get; set; } = 10000; // 10K files per batch
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount * 50; // 50x CPU cores
    public int MaxParallelBatches { get; set; } = 20; // 20 batches simultaneously
    public TimeSpan SasTokenValidity { get; set; } = TimeSpan.FromHours(72); // 3 days for large migrations
    public TimeSpan JobTimeout { get; set; } = TimeSpan.FromHours(6); // 6 hours per job
    public int CopyPollingIntervalMs { get; set; } = 100; // 100ms polling vs 1s
    public bool UseUltraHighPerformanceMode { get; set; } = true;
    
    // Performance metrics
    public long TargetFilesPerHour { get; set; } = 100000; // 100K files/hour target
    public long TargetFilesPerSecond { get; set; } = 28; // ~28 files/second sustained
}

public class BlobFileInfo
{
    public string SourcePath { get; set; } = "";
    public string FileName { get; set; } = "";
    public string Extension { get; set; } = "";
    public long SizeBytes { get; set; }
    public DateTime CreatedOn { get; set; }
    public DateTime ModifiedOn { get; set; }
    public string? TargetPath { get; set; }
    public FileStatus Status { get; set; }
    public string? ErrorMessage { get; set; }
    public string? SharePointUrl { get; set; }
    public string? PackageId { get; set; }
    public string? StagedBlobPath { get; set; }
    
    // Ultra-high performance metrics
    public DateTime? StagingStartTime { get; set; }
    public DateTime? StagingEndTime { get; set; }
    public DateTime? MigrationStartTime { get; set; }
    public DateTime? MigrationEndTime { get; set; }
    public long StagingDurationMs => StagingEndTime.HasValue && StagingStartTime.HasValue 
        ? (long)(StagingEndTime.Value - StagingStartTime.Value).TotalMilliseconds 
        : 0;
    public long MigrationDurationMs => MigrationEndTime.HasValue && MigrationStartTime.HasValue 
        ? (long)(MigrationEndTime.Value - MigrationStartTime.Value).TotalMilliseconds 
        : 0;
}

public class PerformanceMetrics
{
    public long TotalFiles { get; set; }
    public long ProcessedFiles { get; set; }
    public long FailedFiles { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public long ElapsedMs => EndTime.HasValue ? (long)(EndTime.Value - StartTime).TotalMilliseconds : 
        (long)(DateTime.UtcNow - StartTime).TotalMilliseconds;
    public double FilesPerSecond => ProcessedFiles / (ElapsedMs / 1000.0);
    public double PercentComplete => TotalFiles > 0 ? (ProcessedFiles * 100.0 / TotalFiles) : 0;
    public long MegabytesPerSecond => ProcessedFiles > 0 ? 
        (ProcessedFiles * 1024L * 1024L / 10) / (ElapsedMs / 1000) : 0; // Assuming 1MB avg file
}

public class MappingRule
{
    public string SourcePrefix { get; set; } = "";
    public string? TargetPrefix { get; set; }
}

public enum FileStatus
{
    Pending,
    Excluded,
    Staged,
    Migrated,
    Error
}
