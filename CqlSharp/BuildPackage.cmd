@echo off

IF EXIST "C:\NuGetRepository\%1\" GOTO build

C:
cd\
mkdir NuGetRepository\%1\
cd /d %~dp0

:build
nuget pack %~dp0\CqlSharp.csproj -Symbols -Prop Configuration=%1 -OutputDirectory c:\NuGetRepository\%1\


