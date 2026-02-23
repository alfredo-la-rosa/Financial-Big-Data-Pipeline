Write-Host "Initializing MongoDB (market.news)..."

Get-Content scripts/automation/init_mongo.js | docker exec -i mongodb mongosh --quiet

Write-Host "MongoDB initialized."
