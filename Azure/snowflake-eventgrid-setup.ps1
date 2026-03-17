# =====================================================
# SNOWFLAKE EVENT GRID SETUP & HEALTH CHECK
# Author: Tan Nguyen - Data Engineer
# Date: March 17, 2026
# Purpose: Tạo và kiểm tra Event Grid System Topic + Subscription cho Snowpipe
# =====================================================

# =====================================================
# CONFIGURATION - THAY ĐỔI CÁC GIÁ TRỊ NÀY THEO DỰ ÁN
# =====================================================
$ResourceGroupName = "iot-blob"
$StorageAccountName = "iotsnowflake"
$ContainerName = "taxi-blob"
$QueueName = "snowpipeq"
$SystemTopicName = "iot-pipline"
$EventSubscriptionName = "snowpipe-sub"
$Location = "eastus"

Write-Host "🚀 Starting Snowflake Event Grid Pipeline Setup & Health Check..." -ForegroundColor Green
Write-Host "📋 Configuration:" -ForegroundColor Yellow
Write-Host "   RG: $ResourceGroupName" -ForegroundColor Cyan
Write-Host "   Storage: $StorageAccountName" -ForegroundColor Cyan
Write-Host "   Topic: $SystemTopicName" -ForegroundColor Cyan
Write-Host ""

# =====================================================
# BƯỚC 1: LẤY RESOURCE ID
# =====================================================
Write-Host "📍 STEP 1: Getting Storage Account ID..." -ForegroundColor Blue
$STORAGE_ID = az storage account show `
  --name $StorageAccountName `
  --resource-group $ResourceGroupName `
  --query id --output tsv

if (-not $STORAGE_ID) {
    Write-Error "❌ Cannot get Storage Account ID. Check name/RG."
    exit 1
}
Write-Host "✅ Storage ID: $STORAGE_ID" -ForegroundColor Green

# =====================================================
# BƯỚC 2: TẠO SYSTEM TOPIC (nếu chưa có)
# =====================================================
Write-Host "📍 STEP 2: Creating System Topic..." -ForegroundColor Blue
az eventgrid system-topic create `
  --name $SystemTopicName `
  --resource-group $ResourceGroupName `
  --location $Location `
  --topic-type Microsoft.Storage.StorageAccounts `
  --source $STORAGE_ID

Write-Host "✅ System Topic created/verified" -ForegroundColor Green

# =====================================================
# BƯỚC 3: TẠO EVENT SUBSCRIPTION (nếu chưa có)
# =====================================================
Write-Host "📍 STEP 3: Creating Event Subscription..." -ForegroundColor Blue
$QUEUE_ID = "$STORAGE_ID/queueServices/default/queues/$QueueName"

az eventgrid system-topic event-subscription create `
  --name $EventSubscriptionName `
  --resource-group $ResourceGroupName `
  --system-topic-name $SystemTopicName `
  --endpoint-type storagequeue `
  --endpoint $QUEUE_ID `
  --included-event-types Microsoft.Storage.BlobCreated

Write-Host "✅ Event Subscription created" -ForegroundColor Green

# =====================================================
# BƯỚC 4: HEALTH CHECK - KIỂM TRA TRẠNG THÁI
# =====================================================
Write-Host "`n🔍 STEP 4: Health Check..." -ForegroundColor Magenta

# Check System Topic
Write-Host "`n📊 System Topic Status:" -ForegroundColor Yellow
az eventgrid system-topic show `
  --name $SystemTopicName `
  --resource-group $ResourceGroupName `
  --query "{Name:name, Status:provisioningState, Source:source}" `
  --output table

# Check Event Subscription
Write-Host "`n📊 Event Subscription Status:" -ForegroundColor Yellow
az eventgrid system-topic event-subscription show `
  --name $EventSubscriptionName `
  --system-topic-name $SystemTopicName `
  --resource-group $ResourceGroupName `
  --query "{Name:name, Status:provisioningState, Endpoint:destination.endpointType}" `
  --output table

# =====================================================
# BƯỚC 5: TEST END-TO-END
# =====================================================
Write-Host "`n🧪 STEP 5: End-to-End Test..." -ForegroundColor Blue

# Tạo và upload test file
"test,data,1" | Out-File -FilePath "test_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv" -Encoding utf8
$TestFile = Get-ChildItem "test_*.csv"

Write-Host "📤 Uploading test file: $($TestFile.Name)" -ForegroundColor Cyan
az storage blob upload `
  --account-name $StorageAccountName `
  --container-name $ContainerName `
  --name $TestFile.Name `
  --file $TestFile.FullName `
  --auth-mode login

Start-Sleep -Seconds 5

# Check Queue message count
$MessageCount = az storage queue stats `
  --name $QueueName `
  --account-name $StorageAccountName `
  --auth-mode login `
  --query approximateMessageCount `
  --output tsv

Write-Host "`n📊 Queue Message Count: $MessageCount" -ForegroundColor $(if($MessageCount -gt 0) { "Green" } else { "Red" })

# Peek message nếu có
if ([int]$MessageCount -gt 0) {
    Write-Host "`n📄 Queue Message Content (Peek):" -ForegroundColor Yellow
    az storage message peek `
      --queue-name $QueueName `
      --account-name $StorageAccountName `
      --auth-mode login `
      --num-messages 1 `
      --query "[].{ID:id, Content:content}" `
      --output json | ConvertFrom-Json | Format-List
}

# Cleanup test file
Remove-Item $TestFile.FullName

Write-Host "`n🎉 PIPELINE HEALTH CHECK COMPLETE!" -ForegroundColor Green
Write-Host "📋 Next step: Check Snowflake Pipe status:" -ForegroundColor Yellow
Write-Host "   SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_STATUS('YOUR_PIPE_NAME'));" -ForegroundColor Cyan
