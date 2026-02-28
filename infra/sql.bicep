@description('Azure location for SQL resources')
param location string = resourceGroup().location

@description('Globally unique SQL server name')
param sqlServerName string

@description('SQL database name')
param sqlDatabaseName string = 'industryriskdb'

@description('SQL admin username')
param sqlAdminLogin string

@secure()
@description('SQL admin password')
param sqlAdminPassword string

@description('Optional tags')
param tags object = {}

resource sqlServer 'Microsoft.Sql/servers@2023-08-01-preview' = {
  name: sqlServerName
  location: location
  tags: tags
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    publicNetworkAccess: 'Enabled'
    minimalTlsVersion: '1.2'
  }
}

resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-08-01-preview' = {
  name: '${sqlServer.name}/${sqlDatabaseName}'
  location: location
  sku: {
    name: 'Basic'
    tier: 'Basic'
    capacity: 5
  }
  properties: {
    backupStorageRedundancy: 'Local'
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    requestedBackupStorageRedundancy: 'Local'
  }
}

resource allowAzureServices 'Microsoft.Sql/servers/firewallRules@2023-08-01-preview' = {
  name: '${sqlServer.name}/AllowAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

output sqlServerFqdn string = '${sqlServer.name}.database.windows.net'
output sqlDatabaseName string = sqlDatabase.name
