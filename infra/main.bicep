@description('Azure location for the resources')
param location string = resourceGroup().location

@description('Globally unique storage account name (3-24 lower-case alphanumeric)')
param storageAccountName string

@description('Tags applied to resources')
param tags object = {}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
}

resource tableService 'Microsoft.Storage/storageAccounts/tableServices@2023-05-01' = {
  name: '${storageAccount.name}/default'
}

resource hhiTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  name: '${storageAccount.name}/default/hhirisk'
}

resource logisticsTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  name: '${storageAccount.name}/default/logisticsrisk'
}

resource policyTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  name: '${storageAccount.name}/default/policyrisk'
}

output storageAccountId string = storageAccount.id
output storageAccountName string = storageAccount.name
output connectionString string = 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${listKeys(storageAccount.id, storageAccount.apiVersion).keys[0].value};EndpointSuffix=${environment().suffixes.storage}'
