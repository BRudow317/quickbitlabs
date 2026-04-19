Test-NetConnection localhost -Port 1521
Test-NetConnection 127.0.0.1 -Port 1521
Get-Command lsnrctl -ErrorAction SilentlyContinue
Get-Command tnsping -ErrorAction SilentlyContinue
if (Get-Command lsnrctl -ErrorAction SilentlyContinue) { 
    lsnrctl status 
}