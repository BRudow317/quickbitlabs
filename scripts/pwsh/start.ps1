cd $PSScriptRoot/..

& ./scripts/LoadVars.ps1

& ./mvnw.cmd clean package

& ./mvnw.cmd spring-boot:run