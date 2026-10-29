param(
  [Parameter(Mandatory=$true)][string]$Task,
  [Parameter(Mandatory=$true)][string]$OutFile,
  [string]$Patch = $env:DSH_HEADLESS_PATCH,
  [string]$SessionId = "",
  [string]$Cwd = (Get-Location).Path,
  [string]$ToolsMode = "",
  [string]$ErrFile = "",
  [string]$DshExe = $env:DSH_EXE,
  [string]$DshCli = $env:DSH_CLI
)
$ErrorActionPreference = "Continue"
$env:DSH_PERMISSION_MODE = "danger-full-access"
if ($ToolsMode -ne "") { $env:DSH_TOOLS_MODE = $ToolsMode } else { Remove-Item Env:DSH_TOOLS_MODE -ErrorAction SilentlyContinue }
if ($ErrFile -eq "") { $ErrFile = $OutFile + ".stderr.log" }
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutFile) | Out-Null
Set-Location $Cwd
if (-not $Patch) { throw "未指定 Patch：请设置 DSH_HEADLESS_PATCH 或传入 -Patch" }
if (-not $DshExe -or -not $DshCli) { throw "未找到 DSH：请设置 DSH_EXE / DSH_CLI 或传入 -DshExe / -DshCli" }
$exe = $DshExe
$cli = $DshCli
$cliArgs = @("--expose-internals", $cli, "--profile", "headless", "--patch", $Patch, "--json")
if ($SessionId -ne "") { $cliArgs += @("--session-id", $SessionId) }
$cliArgs += @($Task)
# The launcher is a GUI-subsystem executable; a piped stdout is what keeps
# PowerShell waiting for it (plain file redirection returns immediately).
& $exe @cliArgs 2> $ErrFile | Out-File -FilePath $OutFile -Encoding utf8
$code = $LASTEXITCODE
"HEADLESS_EXIT=$code" | Out-File -FilePath ($OutFile + ".exit") -Encoding utf8
exit $code
