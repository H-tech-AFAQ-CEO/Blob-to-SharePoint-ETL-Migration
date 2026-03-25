# Azure Blob to SharePoint ETL Pipeline (.NET)

A .NET 8.0 Console Application that migrates documents from Azure Blob Storage to SharePoint Online using the SharePoint Migration API. Supports PDF, CSV, and HTML files.

---

## Architecture

```
Stage 1 – Inventory      Stage 2 – Transform      Stage 3 – Stage         Stage 4 – Import
─────────────────────    ─────────────────────     ───────────────────    ────────────────────
Azure Blob Storage    →  Apply folder_mapping   →  Copy to staging     →  Submit to Graph
Enumerate & capture      Rename / merge / drop      container              Migration API, poll,
metadata                 folders per mapping CSV                           write report
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| .NET 8.0 SDK | `dotnet --version` |
| Azure Blob Storage | Source container + staging container |
| Azure AD App Registration | Needs `Sites.FullControl.All` Graph permission |
| SharePoint Online | Migration API enabled on tenant |

---

## Setup

```bash
# 1. Build the project
cd BlobToSharePointETL
dotnet restore
dotnet build

# 2. Set environment variables
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."
export BLOB_CONTAINER_NAME="source-docs"
export STAGING_CONTAINER_NAME="spmt-staging"
export SP_TENANT_ID="contoso.onmicrosoft.com"
export SP_CLIENT_ID="<app-registration-client-id>"
export SP_CLIENT_SECRET="<app-registration-secret>"
export SP_SITE_URL="https://contoso.sharepoint.com/sites/mysite"
export SP_DOC_LIBRARY="Documents"

# 3. Edit folder_mapping.csv for your information architecture
```

---

## Running

### Full pipeline
```bash
dotnet run
```

### Dry run (preview mappings)
```bash
dotnet run -- --dry-run
```

### Resume from snapshot
```bash
dotnet run -- --resume logs/inventory_post_stage3_20240601_093000.json --start-stage 4
```

---

## Azure AD App Registration

1. **Azure Portal → Azure Active Directory → App Registrations → New**
2. Note **Application (client) ID** and **Directory (tenant) ID**
3. **Certificates & secrets** → Create client secret
4. **API permissions** → Add `Sites.FullControl.All` (Application permission)
5. **Grant admin consent**
6. In SharePoint Admin Centre, ensure Migration API is enabled

---

## Azure Deployment

### Option 1: Azure Portal
- Create Web App (Container)
- Configure environment variables in App Settings
- Deploy Docker image

### Option 2: Azure CLI
```bash
az group create --name blobtosp-rg --location eastus
az appservice plan create --name blobtosp-plan --resource-group blobtosp-rg --sku B1
az webapp create --name blobtosp-etl --resource-group blobtosp-rg --plan blobtosp-plan \
  --deployment-container-image-name <your-registry>/blobtosp-etl:latest
```

### Option 3: Bicep
```bash
az deployment group create --resource-group blobtosp-rg \
  --template-file BlobToSharePointETL/Deployment/main.bicep \
  --parameters dockerImageName=blobtosp-etl
```

---

## Output

After a run, check the `logs/` directory:

| File | Contents |
|---|---|
| `inventory_post_stage*.json` | State snapshots |
| `report_*.json` | Summary with counts, URLs, failures |

### Sample Report
```json
{
  "generatedAt": "2024-06-01T10:15:00+00:00",
  "totalFiles": 1842,
  "countsByStatus": {
    "Imported": 1819,
    "Excluded": 18,
    "Error": 5
  },
  "imported": [
    { "source": "finance/invoices/2024/inv_001.pdf",
      "sharePointUrl": "https://contoso.sharepoint.com/sites/mysite/Documents/Finance/AP-Invoices/2024/inv_001.pdf" }
  ],
  "failures": [...]
}
```

---

## Project Structure

```
BlobToSharePointETL/
├── Configuration/
│   └── AppConfiguration.cs       # Configuration models
├── Models/
│   ├── BlobFileInfo.cs           # File metadata model
│   └── FolderMapping.cs          # Mapping rule model
├── Services/
│   ├── BlobInventoryService.cs   # Stage 1: Enumerate blobs
│   ├── PathTransformationService.cs  # Stage 2: Apply mappings
│   ├── SharePointMigrationService.cs # Stage 3-4: Migration API
│   └── ReportService.cs          # Reporting & snapshots
├── Deployment/
│   ├── main.bicep                # IaC template
│   ├── azure-pipeline.yaml       # CI/CD pipeline
│   └── azure-config.md           # Portal setup guide
├── appsettings.json              # Default config
├── folder_mapping.csv             # Path transformation rules
├── Dockerfile                     # Container image
└── Program.cs                     # Main entry point
```

---

## Supported File Types

- `.pdf` - PDF documents
- `.csv` - CSV files
- `.html`, `.htm` - HTML documents
