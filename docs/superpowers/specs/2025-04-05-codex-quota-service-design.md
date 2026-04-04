# Codex Quota Service Design

**Date:** 2025-04-05
**Status:** Approved

## Overview

Add a background service for Codex OAuth accounts that:
1. Sends keep-alive requests every 3 hours
2. Checks quota usage every 5 hours
3. Adjusts auth priority based on weekly reset proximity

## Goals

- Keep Codex sessions active to prevent token staleness
- Monitor quota usage to enable intelligent priority-based routing
- Prioritize accounts closer to their weekly reset time

## Non-Goals

- Persisting quota history (in-memory only)
- Exposing quota data via API (future consideration)
- Handling non-OAuth Codex auths (API key auths)

## Architecture

### New File

```
internal/codex/quota_service.go
```

### Modified Files

```
sdk/cliproxy/service.go  - Add codexQuotaService field + startup logic
```

### Package Structure

All functionality consolidated into a single file `internal/codex/quota_service.go`:
- Core types (`CodexQuotaSnapshot`, `CodexUsageWindow`)
- Fetcher function (`FetchCodexQuota`)
- Keep-alive sender (`sendKeepAlive`)
- Priority calculator (`adjustPriorityBasedOnReset`)
- Background service (`CodexQuotaService`)

## Data Types

### CodexQuotaSnapshot

```go
type CodexQuotaSnapshot struct {
    AuthID        string            `json:"auth_id"`
    AccountID     string            `json:"account_id"`
    PlanType      string            `json:"plan_type,omitempty"`
    WeeklyResetAt *time.Time        `json:"weekly_reset_at,omitempty"`
    WeeklyUsedPct float64           `json:"weekly_used_percent,omitempty"`
    RawPayload    map[string]any    `json:"raw_payload,omitempty"`
    FetchedAt     time.Time         `json:"fetched_at"`
    Error         string            `json:"error,omitempty"`
}
```

### CodexUsageWindow (from frontend types)

```go
type CodexUsageWindow struct {
    UsedPercent        float64  `json:"used_percent"`
    LimitWindowSeconds int      `json:"limit_window_seconds"`
    ResetAfterSeconds  int      `json:"reset_after_seconds,omitempty"`
    ResetAt            *time.Time `json:"reset_at,omitempty"`
}
```

## FetchCodexQuota Function

### Token Extraction

| Field | Source | Description |
|-------|--------|-------------|
| `access_token` | `auth.Metadata["access_token"]` | OAuth access token for API calls |
| `id_token` | `auth.Metadata["id_token"]` | JWT containing `chatgpt_account_id` claim |

### Account ID Resolution

Parse `id_token` JWT (base64url decode payload segment) and extract:
- `chatgpt_account_id` or `chatgptAccountId` claim

### API Request

```
GET https://chatgpt.com/backend-api/wham/usage
```

**Headers:**
```
Authorization: Bearer <access_token>
Content-Type: application/json
User-Agent: codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal
Chatgpt-Account-Id: <account_id>
```

### Response Parsing

1. Parse JSON response as `CodexUsagePayload`
2. Navigate to `rate_limit` or `rateLimit`
3. Check `primary_window` and `secondary_window`
4. Find window where `limit_window_seconds == 604800` (weekly)
5. Extract `reset_at` timestamp and `used_percent`

### Error Handling

- Return snapshot with `Error` field populated on failure
- Continue processing other auths on individual failures
- Log errors with auth ID for debugging

## Keep-Alive Function

### Request

Send a minimal chat request to keep session active:

```
POST https://chatgpt.com/backend-api/codex/responses
```

**Headers:** Same as quota fetch plus required Codex headers

**Body:** Minimal "hi" message payload

### Behavior

- Fire-and-forget (don't block on response)
- Log failures but don't retry
- Skip disabled/unavailable auths

## Priority Adjustment Logic

### Rules

| Time until weekly reset | Priority range | Example values |
|-------------------------|----------------|----------------|
| ≤ 48 hours (2 days) | 100-999 | 127, 843, 555 |
| 48-96 hours (2-4 days) | 10-99 | 23, 87, 45 |
| > 96 hours (4+ days) | 1-9 | 3, 7, 1 |

### Implementation

```go
func adjustPriorityBasedOnReset(auth *coreauth.Auth, weeklyResetAt time.Time, rng *rand.Rand) {
    now := time.Now()
    timeUntilReset := weeklyResetAt.Sub(now)

    var priority int
    switch {
    case timeUntilReset <= 48*time.Hour:
        priority = 100 + rng.Intn(900)  // 100-999
    case timeUntilReset <= 96*time.Hour:
        priority = 10 + rng.Intn(90)    // 10-99
    default:
        priority = 1 + rng.Intn(9)      // 1-9
    }

    if auth.Attributes == nil {
        auth.Attributes = make(map[string]string)
    }
    auth.Attributes["priority"] = strconv.Itoa(priority)
}
```

### Randomness

- Use seeded `rand.Rand` per service instance
- Thread-safe random number generation
- Randomness ensures load distribution across auths in same tier

## Background Service

### CodexQuotaService

```go
type CodexQuotaService struct {
    authManager   *coreauth.Manager
    httpClient    *http.Client
    keepAliveInt  time.Duration    // default: 3 hours
    quotaCheckInt time.Duration    // default: 5 hours

    stopCh        chan struct{}
    wg            sync.WaitGroup
    rng           *rand.Rand

    // Optional callbacks
    OnQuotaFetched    func(authID string, snapshot *CodexQuotaSnapshot)
    OnPriorityChanged func(authID string, oldPriority, newPriority int)
}
```

### Lifecycle

1. **Start(ctx)**: Spawn two goroutines with independent tickers
2. **Stop()**: Close stopCh, wait for goroutines to finish

### Keep-Alive Loop (every 3 hours)

```go
func (s *CodexQuotaService) keepAliveLoop(ctx context.Context) {
    ticker := time.NewTicker(s.keepAliveInt)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-s.stopCh:
            return
        case <-ticker.C:
            s.sendKeepAliveToAll(ctx)
        }
    }
}
```

### Quota Check Loop (every 5 hours)

```go
func (s *CodexQuotaService) quotaCheckLoop(ctx context.Context) {
    ticker := time.NewTicker(s.quotaCheckInt)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-s.stopCh:
            return
        case <-ticker.C:
            s.checkQuotaForAll(ctx)
        }
    }
}
```

### Processing All Auths

1. Get all auths from `authManager.SnapshotAuths()`
2. Filter for provider == "codex" and OAuth type (has access_token)
3. Skip disabled/unavailable auths
4. Process concurrently with semaphore limiting

## Integration

### Service Integration

Add to `sdk/cliproxy/service.go`:

```go
type Service struct {
    // ... existing fields ...
    codexQuotaService *codex.CodexQuotaService
}

func (s *Service) startCodexQuotaService(ctx context.Context) {
    if s.codexQuotaService != nil {
        return
    }

    // Check if any Codex OAuth auths exist
    auths := s.coreManager.SnapshotAuths()
    hasCodexOAuth := false
    for _, auth := range auths {
        if strings.ToLower(auth.Provider) == "codex" {
            if auth.Metadata != nil {
                if _, ok := auth.Metadata["access_token"].(string); ok {
                    hasCodexOAuth = true
                    break
                }
            }
        }
    }

    if !hasCodexOAuth {
        return
    }

    s.codexQuotaService = codex.NewCodexQuotaService(
        s.coreManager,
        codex.WithKeepAliveInterval(3*time.Hour),
        codex.WithQuotaCheckInterval(5*time.Hour),
    )
    s.codexQuotaService.Start(ctx)
}
```

### Startup Sequence

1. Service initializes coreManager
2. Auth files loaded and registered
3. `startCodexQuotaService()` called after initial auth load
4. Service starts monitoring if Codex OAuth auths present

### Shutdown Sequence

1. Service.Stop() called
2. Stop codexQuotaService first (to prevent updates during shutdown)
3. Stop other components
4. Wait for all goroutines

## Error Handling

### Individual Auth Failures

- Log error with auth ID and context
- Continue processing other auths
- Retry on next interval (no immediate retry)

### Network Failures

- Use configured HTTP client with timeouts
- Respect context cancellation
- Handle rate limiting (429) gracefully

### Invalid Token

- Log warning
- Skip auth in current cycle
- Will be retried next cycle

## Testing Strategy

### Unit Tests

- `TestFetchCodexQuota`: Mock HTTP responses, verify parsing
- `TestAdjustPriorityBasedOnReset`: Verify priority ranges
- `TestAccountIDExtraction`: JWT parsing edge cases
- `TestWindowClassification`: Weekly vs 5-hour window detection

### Integration Tests

- Service lifecycle (start/stop)
- Concurrent auth processing
- Priority update propagation through auth manager

### Mock Data

Use captured response payloads from frontend testing:
- Plus plan response
- Pro plan response
- Rate limited response
- Empty/missing windows

## Security Considerations

- Access tokens handled in-memory only
- No token logging (mask in logs)
- HTTPS only for API calls
- Respect auth disabled/unavailable flags

## Observability

### Logging

- Service start/stop events
- Keep-alive sent count (debug level)
- Quota fetch results (debug level)
- Priority changes (info level)
- Errors (warn/error level)

### Metrics (Future)

- `codex_keepalive_total` - counter
- `codex_keepalive_errors` - counter
- `codex_quota_fetch_total` - counter
- `codex_quota_fetch_errors` - counter
- `codex_priority_changes` - counter by tier

## References

- Frontend implementation: `src/utils/quota/parsers.ts`, `src/components/quota/quotaConfigs.ts`
- API endpoint: `https://chatgpt.com/backend-api/wham/usage`
- Kiro background refresh pattern: `internal/auth/kiro/background_refresh.go`
