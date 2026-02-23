Write-Host "Initializing Cassandra schema..."

Get-Content scripts/automation/init_cassandra.cql | docker exec -i cassandra cqlsh

Write-Host "Cassandra schema ready."
