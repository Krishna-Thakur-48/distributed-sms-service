Write-Host "========================================="
Write-Host " SMS Service - End-to-End Demo Script"
Write-Host "========================================="

Write-Host "1. Starting Docker containers (Kafka, Zookeeper, MongoDB, Redis)..."
docker-compose up -d
Start-Sleep -Seconds 10 # Wait for services to initialize

Write-Host "2. Adding a test blocked user (7777777777) to Redis..."
docker exec redis redis-cli SADD blocked_users "7777777777" | Out-Null

Write-Host "`n========================================="
Write-Host "ACTION REQUIRED: Start the application services!"
Write-Host "Please open TWO NEW terminal windows:"
Write-Host "  Terminal 1: cd sms-sender-java && .\mvnw.cmd spring-boot:run"
Write-Host "  Terminal 2: cd sms-store-go && go run main.go"
Write-Host "========================================="
Write-Host "Press any key when BOTH services have successfully started up..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

Write-Host "`n3. Sending a SUCCESSFUL SMS request to Java API (8888888888)..."
$response1 = Invoke-RestMethod -Method POST `
  -Uri "http://localhost:8080/v1/sms/send" `
  -ContentType "application/json" `
  -Body '{"phoneNumber":"8888888888","message":"Hello from E2E Demo"}'
$response1 | Format-List

Write-Host "4. Sending an SMS to the BLOCKED number (7777777777)..."
try {
    Invoke-RestMethod -Method POST `
      -Uri "http://localhost:8080/v1/sms/send" `
      -ContentType "application/json" `
      -Body '{"phoneNumber":"7777777777","message":"This should fail"}'
} catch {
    Write-Host "   -> Caught expected error: $_" -ForegroundColor Green
}

Write-Host "`n5. Sending an SMS that simulates a VENDOR FAILURE (+9999999999)..."
$response2 = Invoke-RestMethod -Method POST `
  -Uri "http://localhost:8080/v1/sms/send" `
  -ContentType "application/json" `
  -Body '{"phoneNumber":"+9999999999","message":"Vendor failure test"}'
$response2 | Format-List

Write-Host "6. Waiting a moment for Kafka events to process..."
Start-Sleep -Seconds 3

Write-Host "7. Querying Go service for SMS history of 8888888888..."
$history1 = Invoke-RestMethod -Uri "http://localhost:8081/v1/user/8888888888/messages"
$history1 | Format-List

Write-Host "8. Querying Go service for SMS history of +9999999999 (Failed event should still be saved)..."
$history2 = Invoke-RestMethod -Uri "http://localhost:8081/v1/user/+9999999999/messages"
$history2 | Format-List

Write-Host "Demo complete! To clean up the infrastructure, run: docker-compose down"
