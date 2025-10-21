# This script stops all services for the project.

Write-Host "--- Stopping all Docker services and removing containers... ---" -ForegroundColor Red
docker-compose down

Write-Host "--- Shutdown complete. ---" -ForegroundColor Green
Write-Host "Note: You may need to manually close the Python producer window." -ForegroundColor Yellow