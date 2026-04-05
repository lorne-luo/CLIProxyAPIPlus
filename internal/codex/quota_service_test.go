package codex

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func makeJWTWithPayload(t *testing.T, payload string) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	body := base64.RawURLEncoding.EncodeToString([]byte(payload))
	return header + "." + body + ".sig"
}

func TestExtractCodexAccountID_SnakeCaseClaim(t *testing.T) {
	auth := &coreauth.Auth{Metadata: map[string]any{"id_token": makeJWTWithPayload(t, `{"chatgpt_account_id":"acct_snake"}`)}}
	id, err := extractCodexAccountID(auth)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if id != "acct_snake" {
		t.Fatalf("expected snake claim account id, got %q", id)
	}
}

func TestExtractCodexAccountID_CamelCaseClaim(t *testing.T) {
	auth := &coreauth.Auth{Metadata: map[string]any{"id_token": makeJWTWithPayload(t, `{"chatgptAccountId":"acct_camel"}`)}}
	id, err := extractCodexAccountID(auth)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if id != "acct_camel" {
		t.Fatalf("expected camel claim account id, got %q", id)
	}
}

func TestExtractCodexAccountID_MissingClaim(t *testing.T) {
	auth := &coreauth.Auth{Metadata: map[string]any{"id_token": makeJWTWithPayload(t, `{"foo":"bar"}`)}}
	_, err := extractCodexAccountID(auth)
	if err == nil {
		t.Fatal("expected error for missing account claim")
	}
}

func TestFetchCodexQuota_ParsesWeeklyWindow(t *testing.T) {
	accessToken := "tok123"
	idToken := makeJWTWithPayload(t, `{"chatgptAccountId":"acct_123"}`)
	auth := &coreauth.Auth{ID: "auth-1", Metadata: map[string]any{"access_token": accessToken, "id_token": idToken}}

	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != codexWHAMUsageURL {
			t.Fatalf("unexpected URL: %s", req.URL.String())
		}
		if req.ContentLength > maxQuotaResponseBytes {
			t.Fatalf("request body too large: %d", req.ContentLength)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer "+accessToken {
			t.Fatalf("unexpected auth header: %s", got)
		}
		if got := req.Header.Get("Chatgpt-Account-Id"); got != "acct_123" {
			t.Fatalf("unexpected account header: %s", got)
		}
		body := `{
			"rate_limit": {
				"primary_window": {"limit_window_seconds": 18000, "used_percent": 12.3},
				"secondary_window": {"limit_window_seconds": 604800, "used_percent": 67.5, "reset_at": "2026-04-10T12:00:00Z"}
			}
		}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})

	client := &http.Client{Transport: rt}
	snap := FetchCodexQuota(context.Background(), client, auth)
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	if snap.Error != "" {
		t.Fatalf("unexpected error: %s", snap.Error)
	}
	if snap.AuthID != "auth-1" {
		t.Fatalf("expected auth id auth-1, got %s", snap.AuthID)
	}
	if snap.AccountID != "acct_123" {
		t.Fatalf("expected account id acct_123, got %s", snap.AccountID)
	}
	if snap.WeeklyUsedPct != 67.5 {
		t.Fatalf("expected weekly pct 67.5, got %v", snap.WeeklyUsedPct)
	}
	if snap.WeeklyResetAt == nil || snap.WeeklyResetAt.IsZero() {
		t.Fatal("expected weekly reset time")
	}
}

func TestAdjustPriorityBasedOnReset_RangeBands(t *testing.T) {
	svc := NewCodexQuotaService(nil, WithRandSource(rand.NewSource(1)))
	auth := &coreauth.Auth{Attributes: map[string]string{}}
	now := time.Now()

	_, pNear, _ := svc.adjustPriorityBasedOnReset(auth, now.Add(24*time.Hour))
	if pNear < 100 || pNear > 999 {
		t.Fatalf("near-reset priority out of range: %d", pNear)
	}

	_, pMid, _ := svc.adjustPriorityBasedOnReset(auth, now.Add(72*time.Hour))
	if pMid < 10 || pMid > 99 {
		t.Fatalf("mid-reset priority out of range: %d", pMid)
	}

	_, pFar, _ := svc.adjustPriorityBasedOnReset(auth, now.Add(120*time.Hour))
	if pFar < 1 || pFar > 9 {
		t.Fatalf("far-reset priority out of range: %d", pFar)
	}
}

func TestEligibleCodexOAuthAuths_Filtering(t *testing.T) {
	auths := []*coreauth.Auth{
		{ID: "ok", Provider: "codex", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "tok"}},
		{ID: "no-provider", Provider: "claude", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "tok"}},
		{ID: "disabled", Provider: "codex", Status: coreauth.StatusActive, Disabled: true, Metadata: map[string]any{"access_token": "tok"}},
		{ID: "unavailable", Provider: "codex", Status: coreauth.StatusActive, Unavailable: true, Metadata: map[string]any{"access_token": "tok"}},
		{ID: "inactive", Provider: "codex", Status: coreauth.StatusError, Metadata: map[string]any{"access_token": "tok"}},
		{ID: "no-token", Provider: "codex", Status: coreauth.StatusActive, Metadata: map[string]any{"other": "x"}},
	}
	result := eligibleCodexOAuthAuths(auths)
	if len(result) != 1 || result[0].ID != "ok" {
		ids := make([]string, 0, len(result))
		for _, a := range result {
			ids = append(ids, a.ID)
		}
		t.Fatalf("unexpected eligible auths: %v", ids)
	}
}

func TestSendKeepAlive_BuildsRequest(t *testing.T) {
	accessToken := "tok123"
	idToken := makeJWTWithPayload(t, `{"chatgptAccountId":"acct_999"}`)
	auth := &coreauth.Auth{ID: "auth-keepalive", Metadata: map[string]any{"access_token": accessToken, "id_token": idToken}}

	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != codexResponsesURL {
			t.Fatalf("unexpected URL: %s", req.URL.String())
		}
		if req.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer "+accessToken {
			t.Fatalf("unexpected auth header: %s", got)
		}
		if got := req.Header.Get("Chatgpt-Account-Id"); got != "acct_999" {
			t.Fatalf("unexpected account header: %s", got)
		}
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("failed reading request body: %v", err)
		}
		if !strings.Contains(string(payload), "hi") {
			t.Fatalf("expected payload to contain hi, got %s", string(payload))
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})}

	if err := sendKeepAlive(context.Background(), client, auth); err != nil {
		t.Fatalf("sendKeepAlive returned error: %v", err)
	}
}

func TestCodexQuotaServiceStartStop_Idempotent(t *testing.T) {
	svc := NewCodexQuotaService(nil, WithKeepAliveInterval(time.Hour), WithQuotaCheckInterval(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc.Start(ctx)
	svc.Start(ctx)
	if !svc.started.Load() {
		t.Fatal("expected service to be marked started")
	}

	done := make(chan struct{})
	go func() {
		svc.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("service stop timed out")
	}

	// second stop should not panic
	svc.Stop()
}

func TestParseWHAMUsageWindow_CamelCase(t *testing.T) {
	resetAt := "2026-04-09T10:00:00Z"
	payload := map[string]any{
		"rateLimit": map[string]any{
			"primaryWindow": map[string]any{"limitWindowSeconds": float64(604800), "usedPercent": 44.1, "resetAt": resetAt},
		},
	}
	window, ok := parseWHAMUsageWindow(payload)
	if !ok {
		t.Fatal("expected weekly window match")
	}
	if window.LimitWindowSeconds != 604800 {
		t.Fatalf("unexpected window size: %d", window.LimitWindowSeconds)
	}
	if fmt.Sprintf("%.1f", window.UsedPercent) != "44.1" {
		t.Fatalf("unexpected used percent: %v", window.UsedPercent)
	}
	if window.ResetAt == nil || window.ResetAt.IsZero() {
		t.Fatal("expected resetAt to parse")
	}
}

func TestParsePriority(t *testing.T) {
	if got := parsePriority(nil); got != 0 {
		t.Fatalf("expected 0 for nil, got %d", got)
	}
	if got := parsePriority(map[string]string{"priority": "x"}); got != 0 {
		t.Fatalf("expected 0 for invalid, got %d", got)
	}
	if got := parsePriority(map[string]string{"priority": "15"}); got != 15 {
		t.Fatalf("expected 15, got %d", got)
	}
}

func TestPriorityStringValue(t *testing.T) {
	svc := NewCodexQuotaService(nil, WithRandSource(rand.NewSource(2)))
	auth := &coreauth.Auth{Attributes: map[string]string{"priority": "1"}}
	_, newPriority, changed := svc.adjustPriorityBasedOnReset(auth, time.Now().Add(24*time.Hour))
	if !changed {
		t.Fatalf("expected priority to change")
	}
	if auth.Attributes["priority"] != strconv.Itoa(newPriority) {
		t.Fatalf("priority string mismatch: %s != %d", auth.Attributes["priority"], newPriority)
	}
}
