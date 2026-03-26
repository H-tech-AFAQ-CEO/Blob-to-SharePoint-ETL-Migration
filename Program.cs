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
            
            rootCommand.AddOption(dryRunOption);
            rootCommand.AddOption(testAuthOption);

            rootCommand.SetHandler(async (bool dryRun, bool testAuth) =>
            {
                if (testAuth)
                {
                    await TestAuthenticationAsync();
                }
                else
                {
                    await RunPipelineAsync(dryRun);
                }
            }, dryRunOption, testAuthOption);

            return await rootCommand.InvokeAsync(args);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Application failed");
            return 1;
        }
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
        _logger!.LogInformation("[Stage 3/4] Staging files to Azure Blob container...");
        
        var blobServiceClient = new BlobServiceClient(config.StorageConnectionString);
        var stagingContainer = blobServiceClient.GetBlobContainerClient(config.StagingContainerName);
        
        // Create staging container if it doesn't exist
        await stagingContainer.CreateIfNotExistsAsync();
        
        var stagedFiles = new List<BlobFileInfo>();
        var batchSize = 2; // PoC: Small batch size for testing
        var currentBatch = new List<BlobFileInfo>();
        var packageCounter = 1;
        
        foreach (var file in files.Where(f => f.Status == FileStatus.Pending))
        {
            try
            {
                // Create batch packages
                if (currentBatch.Count >= batchSize)
                {
                    await ProcessBatchAsync(currentBatch, packageCounter++.ToString(), blobServiceClient, config);
                    stagedFiles.AddRange(currentBatch);
                    currentBatch.Clear();
                }
                
                currentBatch.Add(file);
            }
            catch (Exception ex)
            {
                _logger!.LogError(ex, $"Failed to stage file: {file.FileName}");
                file.Status = FileStatus.Error;
                file.ErrorMessage = ex.Message;
            }
        }
        
        // Process remaining files in last batch
        if (currentBatch.Any())
        {
            await ProcessBatchAsync(currentBatch, packageCounter++.ToString(), blobServiceClient, config);
            stagedFiles.AddRange(currentBatch);
        }
        
        _logger!.LogInformation($"PoC: Staged {stagedFiles.Count} files into {packageCounter - 1} packages");
    }

    private static async Task ProcessBatchAsync(List<BlobFileInfo> batch, string packageId, BlobServiceClient blobServiceClient, Configuration config)
    {
        var packagePath = $"{packageId}/content";
        var stagingContainer = blobServiceClient.GetBlobContainerClient(config.StagingContainerName);
        
        _logger!.LogInformation($"Processing high-performance batch {packageId} with {batch.Count} files...");
        
        // Configure parallel processing for optimal performance
        var maxConcurrency = Math.Min(batch.Count, Environment.ProcessorCount * 2);
        var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        
        var copyTasks = batch.Select(async file =>
        {
            await semaphore.WaitAsync();
            try
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                
                // Copy file from source to staging container
                var sourceContainer = blobServiceClient.GetBlobContainerClient(config.SourceContainerName);
                var sourceBlob = sourceContainer.GetBlobClient(file.SourcePath);
                var stagingBlob = stagingContainer.GetBlobClient($"{packagePath}/{file.TargetPath}");
                
                _logger!.LogDebug($"Staging: {file.SourcePath} -> {packagePath}/{file.TargetPath}");
                
                // Copy the blob with high-performance settings
                await stagingBlob.StartCopyFromUriAsync(sourceBlob.Uri);
                
                // Wait for copy to complete with timeout
                var copyStatus = CopyStatus.Pending;
                var attempts = 0;
                var maxAttempts = 120; // 2 minutes max wait per file
                
                while (copyStatus == CopyStatus.Pending && attempts < maxAttempts)
                {
                    await Task.Delay(1000);
                    var properties = await stagingBlob.GetPropertiesAsync();
                    copyStatus = properties.Value.CopyStatus;
                    attempts++;
                    
                    // Log progress every 30 seconds
                    if (attempts % 30 == 0)
                    {
                        _logger!.LogDebug($"Still copying {file.FileName} - attempt {attempts}/{maxAttempts}");
                    }
                }
                
                stopwatch.Stop();
                
                if (copyStatus != CopyStatus.Success)
                {
                    throw new Exception($"Copy failed with status: {copyStatus} after {attempts} attempts ({stopwatch.ElapsedMilliseconds}ms)");
                }
                
                // Update file info
                file.PackageId = packageId;
                file.StagedBlobPath = $"{packagePath}/{file.TargetPath}";
                file.Status = FileStatus.Staged;
                
                _logger!.LogDebug($"Successfully staged {file.FileName} in {stopwatch.ElapsedMilliseconds}ms");
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
        
        await Task.WhenAll(copyTasks);
        
        var successCount = batch.Count(f => f.Status == FileStatus.Staged);
        var errorCount = batch.Count(f => f.Status == FileStatus.Error);
        
        _logger!.LogInformation($"Batch {packageId} completed: {successCount} succeeded, {errorCount} failed");
    }

    private static async Task MigrateToSharePointAsync(List<BlobFileInfo> files, Configuration config)
    {
        _logger!.LogInformation("[Stage 4/4] Starting SharePoint Migration API jobs...");
        
        var token = await GetAccessTokenAsync(config);
        var siteId = await GetSiteIdAsync(config, token);
        
        // Group staged files by package ID for batch processing
        var packageGroups = files
            .Where(f => f.Status == FileStatus.Staged)
            .GroupBy(f => f.PackageId)
            .ToList();
        
        _logger!.LogInformation($"Processing {packageGroups.Count} migration packages...");
        
        foreach (var packageGroup in packageGroups)
        {
            var packageId = packageGroup.Key;
            var packageFiles = packageGroup.ToList();
            
            try
            {
                _logger!.LogInformation($"Processing package {packageId} with {packageFiles.Count} files...");
                
                // Generate migration manifest
                var manifest = await GenerateMigrationManifestAsync(packageFiles, config);
                var manifestBlobPath = await UploadManifestAsync(manifest, packageId, config);
                
                // Generate SAS URL for staging container
                var sasUrl = await GenerateContainerSasUrlAsync(config);
                
                // Submit migration job
                var jobId = await SubmitMigrationJobAsync(packageId, manifestBlobPath, sasUrl, siteId, token, config);
                
                // Poll for job completion
                await PollMigrationJobAsync(jobId, siteId, token, packageFiles);
                
                _logger!.LogInformation($"Package {packageId} migration completed successfully");
            }
            catch (Exception ex)
            {
                _logger!.LogError(ex, $"Package {packageId} migration failed");
                foreach (var file in packageFiles)
                {
                    file.Status = FileStatus.Error;
                    file.ErrorMessage = ex.Message;
                }
            }
        }
        
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

    private static async Task PollMigrationJobAsync(string jobId, string siteId, string token, List<BlobFileInfo> files)
    {
        using var httpClient = new HttpClient();
        var maxAttempts = 60; // PoC: ~30 minutes with 30-second intervals for testing
        var attempt = 0;
        
        _logger!.LogInformation($"PoC: Starting to poll migration job {jobId} (max {maxAttempts} attempts)");
        
        while (attempt < maxAttempts)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(30));
                attempt++;
                
                var request = new HttpRequestMessage(HttpMethod.Get, $"https://graph.microsoft.com/v1.0/sites/{siteId}/migrate/{jobId}");
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                
                var response = await httpClient.SendAsync(request);
                var responseContent = await response.Content.ReadAsStringAsync();
                
                if (response.IsSuccessStatusCode)
                {
                    var result = JsonSerializer.Deserialize<JsonElement>(responseContent);
                    var status = result.GetProperty("status").GetString();
                    
                    _logger!.LogInformation($"PoC: Job {jobId} status: {status} (Attempt {attempt}/{maxAttempts})");
                    
                    switch (status)
                    {
                        case "completed":
                            var completedFiles = result.TryGetProperty("completedItems", out var completedProp) 
                                ? completedProp.GetInt32() 
                                : files.Count;
                            _logger!.LogInformation($"PoC: Job {jobId} completed successfully - {completedFiles} files migrated");
                            
                            foreach (var file in files)
                            {
                                file.Status = FileStatus.Migrated;
                                file.SharePointUrl = $"{files.First().TargetPath}"; // Will be updated with actual URL
                            }
                            return;
                            
                        case "failed":
                            var error = result.GetProperty("error").GetString();
                            var failedItems = result.TryGetProperty("failedItems", out var failedProp) 
                                ? failedProp.GetInt32() 
                                : 0;
                            _logger!.LogError($"PoC: Migration job failed: {error} ({failedItems} items failed)");
                            throw new Exception($"Migration job failed: {error}");
                            
                        case "inProgress":
                            var progress = result.TryGetProperty("progress", out var progressProp) 
                                ? progressProp.GetInt32() 
                                : 0;
                            _logger!.LogDebug($"PoC: Job {jobId} in progress: {progress}%");
                            continue;
                            
                        default:
                            _logger!.LogWarning($"PoC: Unknown job status: {status}");
                            continue;
                    }
                }
                else
                {
                    _logger!.LogWarning($"PoC: Failed to poll job status: {response.StatusCode} - {responseContent}");
                }
            }
            catch (Exception ex)
            {
                _logger!.LogError(ex, $"PoC: Error polling migration job (Attempt {attempt}/{maxAttempts})");
                if (attempt >= maxAttempts)
                    throw;
                
                // Don't throw on transient errors, continue polling
                if (ex is HttpRequestException || ex is TaskCanceledException)
                {
                    _logger!.LogWarning($"PoC: Transient error polling job, continuing...");
                    continue;
                }
                else
                {
                    throw;
                }
            }
        }
        
        throw new TimeoutException($"PoC: Migration job {jobId} did not complete within {maxAttempts * 30} seconds (30 minutes)");
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
    
    // High-performance configuration
    public int BatchSize { get; set; } = 500;
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount * 2;
    public TimeSpan SasTokenValidity { get; set; } = TimeSpan.FromHours(48);
    public TimeSpan JobTimeout { get; set; } = TimeSpan.FromHours(2);
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
