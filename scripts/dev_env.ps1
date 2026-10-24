# Prepare this PowerShell window for work on the project. Dot-source it from the
# repository root:
#
#     . .\scripts\dev_env.ps1
#
# It sets environment variables for this window only, so other projects and new windows
# keep whatever Java and Python they had.

$repo = Split-Path -Parent $PSScriptRoot
$jdk = Join-Path $repo ".tools\jdk-17"
if (-not (Test-Path (Join-Path $jdk "bin\java.exe"))) {
    throw "No JDK found at $jdk. Follow the 'Java' section of README.md first."
}

# PySpark starts the JVM from JAVA_HOME.
$env:JAVA_HOME = $jdk
$env:PATH = "$jdk\bin;$env:PATH"

. (Join-Path $repo ".venv\Scripts\Activate.ps1")
