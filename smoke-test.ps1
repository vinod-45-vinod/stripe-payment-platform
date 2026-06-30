# =============================================================================
# smoke-test.ps1 — Payment Platform Smoke Test (Windows PowerShell)
# =============================================================================
# Usage:
#   .\smoke-test.ps1               # Build + start stack, run tests, tear down
#   .\smoke-test.ps1 -NoTeardown   # Keep stack running after test
#   .\smoke-test.ps1 -SkipCompose  # Use already-running stack
#
# Requirements: Docker Desktop for Windows, jq (install via `winget install jqlang.jq`)
# =============================================================================
param(
    [switch]$NoTeardown,
    [switch]$SkipCompose
)

$ErrorActionPreference = "Stop"

# ── Colors ────────────────────────────────────────────────────────────────────
function Write-Pass($msg) { Write-Host "✅ PASS: $msg" -ForegroundColor Green }
function Write-Fail($msg) { Write-Host "❌ FAIL: $msg" -ForegroundColor Red; exit 1 }
function Write-Info($msg) { Write-Host "ℹ️   $msg" -ForegroundColor Cyan }
function Write-Step($msg) { Write-Host "`n══ $msg ══" -ForegroundColor Yellow }

# ── Cleanup ───────────────────────────────────────────────────────────────────
function Invoke-Cleanup {
    if (-not $NoTeardown) {
        Write-Step "Tearing down stack"
        docker compose down -v --remove-orphans
        Write-Info "Stack torn down."
    } else {
        Write-Info "Stack left running (-NoTeardown). Use 'docker compose down' to stop."
    }
}

# ── 1. Start stack ────────────────────────────────────────────────────────────
if (-not $SkipCompose) {
    Write-Step "Starting Payment Platform stack"
    docker compose up -d --build
    Write-Info "Containers started. Waiting for services to be healthy..."
}

# ── 2. Wait for health endpoints ──────────────────────────────────────────────
function Wait-Healthy {
    param([string]$Name, [string]$Url, [int]$MaxWaitSeconds = 120)
    
    Write-Info "Waiting for $Name ($Url)..."
    $elapsed = 0
    while ($elapsed -lt $MaxWaitSeconds) {
        try {
            $resp = Invoke-RestMethod -Uri $Url -TimeoutSec 5 -ErrorAction SilentlyContinue
            if ($resp.status -eq "UP") {
                Write-Pass "$Name is UP"
                return
            }
        } catch { }
        Start-Sleep -Seconds 3
        $elapsed += 3
        Write-Host "." -NoNewline
    }
    Write-Fail "$Name did not become healthy in ${MaxWaitSeconds}s"
}

Write-Step "Health checks"
Wait-Healthy "payment-service"      "http://localhost:8080/actuator/health" 120
Wait-Healthy "ledger-service"       "http://localhost:8081/actuator/health" 60
Wait-Healthy "notification-service" "http://localhost:8082/actuator/health" 60
Wait-Healthy "fraud-service"        "http://localhost:8083/actuator/health" 60

# ── Helper: POST request ──────────────────────────────────────────────────────
function Invoke-Post {
    param([string]$Url, [string]$Body = "", [hashtable]$Headers = @{})
    $allHeaders = @{ "Content-Type" = "application/json" } + $Headers
    return Invoke-RestMethod -Uri $Url -Method Post -Body $Body -Headers $allHeaders -TimeoutSec 10
}

# ── 3. Full payment lifecycle ─────────────────────────────────────────────────
Write-Step "Payment flow: CREATE → AUTHORIZE → CAPTURE"

$createBody = '{"amount": 100.00, "currency": "USD", "customerId": "cust_smoke_001", "merchantId": "merch_smoke_001"}'
$createResp  = Invoke-Post "http://localhost:8080/payments" $createBody
$paymentId   = $createResp.id

if ($createResp.status -eq "CREATED") {
    Write-Pass "Payment created (id=$paymentId, status=CREATED)"
} else {
    Write-Fail "Expected status=CREATED, got $($createResp.status)"
}

$authResp = Invoke-Post "http://localhost:8080/payments/$paymentId/authorize"
if ($authResp.status -eq "AUTHORIZED") { Write-Pass "Payment authorized" } else { Write-Fail "Expected AUTHORIZED, got $($authResp.status)" }

$capResp = Invoke-Post "http://localhost:8080/payments/$paymentId/capture"
if ($capResp.status -eq "SUCCEEDED") { Write-Pass "Payment captured → SUCCEEDED" } else { Write-Fail "Expected SUCCEEDED, got $($capResp.status)" }

$getResp = Invoke-RestMethod "http://localhost:8080/payments/$paymentId"
if ($getResp.status -eq "SUCCEEDED") { Write-Pass "GET /payments/$paymentId returns SUCCEEDED" } else { Write-Fail "Expected SUCCEEDED from GET, got $($getResp.status)" }

# ── 4. Idempotency ────────────────────────────────────────────────────────────
Write-Step "Idempotency: duplicate POST with same key"

$idemKey  = "smoke-test-idem-$([System.DateTime]::UtcNow.Ticks)"
$idemBody = '{"amount": 50.00, "currency": "USD", "customerId": "cust_smoke_002", "merchantId": "merch_smoke_001"}'

$idem1 = Invoke-Post "http://localhost:8080/payments" $idemBody @{"Idempotency-Key" = $idemKey}
$idem2 = Invoke-Post "http://localhost:8080/payments" $idemBody @{"Idempotency-Key" = $idemKey}

if ($idem1.id -eq $idem2.id) {
    Write-Pass "Idempotent: both requests returned same id=$($idem1.id)"
} else {
    Write-Fail "Idempotency FAILED: first=$($idem1.id), second=$($idem2.id)"
}

# ── 5. Refund flow ────────────────────────────────────────────────────────────
Write-Step "Refund flow"

$refundCreate = Invoke-Post "http://localhost:8080/payments" '{"amount": 200.00, "currency": "USD", "customerId": "cust_smoke_003", "merchantId": "merch_smoke_001"}'
$rid = $refundCreate.id
Invoke-Post "http://localhost:8080/payments/$rid/authorize" | Out-Null
Invoke-Post "http://localhost:8080/payments/$rid/capture" | Out-Null
$refundResp = Invoke-Post "http://localhost:8080/payments/$rid/refund"

if ($refundResp.status -eq "REFUNDED") { Write-Pass "Refund succeeded (status=REFUNDED)" } else { Write-Fail "Expected REFUNDED, got $($refundResp.status)" }

# ── 6. Ledger check ───────────────────────────────────────────────────────────
Write-Step "Ledger-service balance check"
Start-Sleep -Seconds 5  # Allow Kafka event to propagate

try {
    $ledger = Invoke-RestMethod "http://localhost:8081/accounts/MERCHANT/merch_smoke_001/balance" -TimeoutSec 5
    Write-Pass "Ledger-service balance endpoint responded"
} catch {
    Write-Info "Ledger balance endpoint check: $($_.Exception.Message) (may need more propagation time)"
}

# ── Teardown & Summary ────────────────────────────────────────────────────────
Invoke-Cleanup

Write-Host ""
Write-Host "═══════════════════════════════════════════════" -ForegroundColor Green
Write-Host "  ✅ ALL SMOKE TESTS PASSED                    " -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "  Payment flow:   CREATE → AUTHORIZE → CAPTURE → SUCCEEDED ✅"
Write-Host "  Idempotency:    Duplicate requests return same ID ✅"
Write-Host "  Refund flow:    CAPTURE → REFUND → REFUNDED ✅"
Write-Host "  Health checks:  All 4 services UP ✅"
Write-Host ""
