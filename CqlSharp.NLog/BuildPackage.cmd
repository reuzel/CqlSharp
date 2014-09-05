@echo off
C:
cd\

IF EXIST "C:\NuGetRepository\%1\" GOTO build
mkdir NuGetRepository\%1\

:build
nuget pack %~dp0\CqlSharp.NLog.csproj -Symbols -Prop Configuration=%1 -OutputDirectory c:\NuGetRepository\%1\


