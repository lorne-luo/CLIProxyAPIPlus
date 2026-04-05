package codex

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	authcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	defaultKeepAliveInterval  = 3 * time.Hour
	defaultQuotaCheckInterval = 5 * time.Hour
	defaultConcurrency        = 6
	defaultRequestTimeout     = 30 * time.Second
	maxQuotaResponseBytes     = 256 << 10
	weeklyWindowSeconds       = 604800

	codexWHAMUsageURL = "https://chatgpt.com/backend-api/wham/usage"
	codexResponsesURL = "https://chatgpt.com/backend-api/codex/responses"

	codexUserAgent  = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
	codexOriginator = "codex_cli_rs"
)

type CodexQuotaSnapshot struct {
	AuthID        string     `json:"auth_id"`
	AccountID     string     `json:"account_id"`
	PlanType      string     `json:"plan_type,omitempty"`
	WeeklyResetAt *time.Time `json:"weekly_reset_at,omitempty"`
	WeeklyUsedPct float64    `json:"weekly_used_percent,omitempty"`
	FetchedAt     time.Time  `json:"fetched_at"`
	Error         string     `json:"error,omitempty"`
}

type CodexUsageWindow struct {
	UsedPercent        float64    `json:"used_percent"`
	LimitWindowSeconds int        `json:"limit_window_seconds"`
	ResetAfterSeconds  int        `json:"reset_after_seconds,omitempty"`
	ResetAt            *time.Time `json:"reset_at,omitempty"`
}

type CodexQuotaService struct {
	authManager   *coreauth.Manager
	httpClient    *http.Client
	keepAliveInt  time.Duration
	quotaCheckInt time.Duration
	concurrency   int

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	started  atomic.Bool

	rng   *rand.Rand
	rngMu sync.Mutex

	OnQuotaFetched    func(authID string, snapshot *CodexQuotaSnapshot)
	OnPriorityChanged func(authID string, oldPriority, newPriority int)
}

type Option func(*CodexQuotaService)

func WithKeepAliveInterval(d time.Duration) Option {
	return func(s *CodexQuotaService) {
		if d > 0 {
			s.keepAliveInt = d
		}
	}
}

func WithQuotaCheckInterval(d time.Duration) Option {
	return func(s *CodexQuotaService) {
		if d > 0 {
			s.quotaCheckInt = d
		}
	}
}

func WithHTTPClient(c *http.Client) Option {
	return func(s *CodexQuotaService) {
		if c != nil {
			s.httpClient = c
		}
	}
}

func WithConcurrency(n int) Option {
	return func(s *CodexQuotaService) {
		if n > 0 {
			s.concurrency = n
		}
	}
}

func WithRandSource(src rand.Source) Option {
	return func(s *CodexQuotaService) {
		if src != nil {
			s.rng = rand.New(src)
		}
	}
}

func NewCodexQuotaService(authManager *coreauth.Manager, opts ...Option) *CodexQuotaService {
	svc := &CodexQuotaService{
		authManager:   authManager,
		httpClient:    &http.Client{Timeout: defaultRequestTimeout},
		keepAliveInt:  defaultKeepAliveInterval,
		quotaCheckInt: defaultQuotaCheckInterval,
		concurrency:   defaultConcurrency,
		stopCh:        make(chan struct{}),
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(svc)
		}
	}
	return svc
}

func (s *CodexQuotaService) Start(ctx context.Context) {
	if s == nil {
		return
	}
	if !s.started.CompareAndSwap(false, true) {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.keepAliveLoop(ctx)
	}()
	go func() {
		defer s.wg.Done()
		s.quotaCheckLoop(ctx)
	}()
}

func (s *CodexQuotaService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
}

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

func (s *CodexQuotaService) sendKeepAliveToAll(ctx context.Context) {
	if s == nil || s.authManager == nil {
		return
	}
	auths := eligibleCodexOAuthAuths(s.authManager.List())
	if len(auths) == 0 {
		return
	}

	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-s.stopCh:
			wg.Wait()
			return
		default:
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(a *coreauth.Auth) {
			defer wg.Done()
			defer func() { <-sem }()

			reqCtx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
			defer cancel()
			if err := sendKeepAlive(reqCtx, s.httpClient, a); err != nil {
				log.WithField("auth_id", a.ID).Warnf("codex keep-alive failed: %v", err)
			}
		}(auth)
	}
	wg.Wait()
}

func (s *CodexQuotaService) checkQuotaForAll(ctx context.Context) {
	if s == nil || s.authManager == nil {
		return
	}
	auths := eligibleCodexOAuthAuths(s.authManager.List())
	if len(auths) == 0 {
		return
	}

	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-s.stopCh:
			wg.Wait()
			return
		default:
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(a *coreauth.Auth) {
			defer wg.Done()
			defer func() { <-sem }()

			reqCtx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
			defer cancel()

			snapshot := FetchCodexQuota(reqCtx, s.httpClient, a)
			if s.OnQuotaFetched != nil {
				s.OnQuotaFetched(a.ID, snapshot)
			}
			if snapshot == nil {
				return
			}
			if snapshot.Error != "" {
				log.WithField("auth_id", a.ID).Warnf("codex quota fetch failed: %s", snapshot.Error)
				return
			}
			if snapshot.WeeklyResetAt == nil || snapshot.WeeklyResetAt.IsZero() {
				return
			}

			current, ok := s.authManager.GetByID(a.ID)
			if !ok || current == nil {
				return
			}
			oldPriority, newPriority, changed := s.adjustPriorityBasedOnReset(current, *snapshot.WeeklyResetAt)
			if !changed {
				return
			}
			if _, err := s.authManager.Update(reqCtx, current); err != nil {
				log.WithField("auth_id", a.ID).Warnf("codex priority update failed: %v", err)
				return
			}
			if s.OnPriorityChanged != nil {
				s.OnPriorityChanged(a.ID, oldPriority, newPriority)
			}
			log.WithField("auth_id", a.ID).Infof("codex auth priority updated: %d -> %d", oldPriority, newPriority)
		}(auth)
	}
	wg.Wait()
}

func FetchCodexQuota(ctx context.Context, client *http.Client, auth *coreauth.Auth) *CodexQuotaSnapshot {
	snapshot := &CodexQuotaSnapshot{
		FetchedAt: time.Now().UTC(),
	}
	if auth != nil {
		snapshot.AuthID = auth.ID
		if auth.Attributes != nil {
			snapshot.PlanType = strings.TrimSpace(auth.Attributes["plan_type"])
		}
	}
	if auth == nil {
		snapshot.Error = "nil auth"
		return snapshot
	}
	accessToken := accessTokenFromAuth(auth)
	if accessToken == "" {
		snapshot.Error = "missing access_token"
		return snapshot
	}
	accountID, err := extractCodexAccountID(auth)
	if err != nil {
		snapshot.Error = err.Error()
		return snapshot
	}
	snapshot.AccountID = accountID
	if client == nil {
		client = &http.Client{Timeout: defaultRequestTimeout}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, codexWHAMUsageURL, nil)
	if err != nil {
		snapshot.Error = fmt.Sprintf("build request: %v", err)
		return snapshot
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", codexUserAgent)
	req.Header.Set("Chatgpt-Account-Id", accountID)
	req.Header.Set("Originator", codexOriginator)

	resp, err := client.Do(req)
	if err != nil {
		snapshot.Error = fmt.Sprintf("request failed: %v", err)
		return snapshot
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxQuotaResponseBytes))
	if err != nil {
		snapshot.Error = fmt.Sprintf("read response: %v", err)
		return snapshot
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snapshot.Error = fmt.Sprintf("unexpected status %d", resp.StatusCode)
		return snapshot
	}

	payload := map[string]any{}
	if err := json.Unmarshal(body, &payload); err != nil {
		snapshot.Error = fmt.Sprintf("decode payload: %v", err)
		return snapshot
	}

	window, ok := parseWHAMUsageWindow(payload)
	if !ok {
		snapshot.Error = "weekly window not found"
		return snapshot
	}
	snapshot.WeeklyUsedPct = window.UsedPercent
	snapshot.WeeklyResetAt = window.ResetAt
	return snapshot
}

func sendKeepAlive(ctx context.Context, client *http.Client, auth *coreauth.Auth) error {
	if auth == nil {
		return fmt.Errorf("nil auth")
	}
	accessToken := accessTokenFromAuth(auth)
	if accessToken == "" {
		return fmt.Errorf("missing access_token")
	}
	accountID, err := extractCodexAccountID(auth)
	if err != nil {
		return err
	}
	if client == nil {
		client = &http.Client{Timeout: defaultRequestTimeout}
	}

	payload := map[string]any{
		"model": "gpt-5",
		"input": []map[string]any{{
			"role":    "user",
			"content": "hi",
		}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal keepalive payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, codexResponsesURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build keepalive request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", codexUserAgent)
	req.Header.Set("Chatgpt-Account-Id", accountID)
	req.Header.Set("Originator", codexOriginator)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send keepalive request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("keepalive status %d", resp.StatusCode)
	}
	return nil
}

func (s *CodexQuotaService) adjustPriorityBasedOnReset(auth *coreauth.Auth, weeklyResetAt time.Time) (oldPriority, newPriority int, changed bool) {
	if auth == nil {
		return 0, 0, false
	}
	oldPriority = parsePriority(auth.Attributes)
	newPriority = s.nextPriorityInTier(time.Until(weeklyResetAt))
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["priority"] = strconv.Itoa(newPriority)
	return oldPriority, newPriority, oldPriority != newPriority
}

func (s *CodexQuotaService) nextPriorityInTier(untilReset time.Duration) int {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	switch {
	case untilReset <= 48*time.Hour:
		return 100 + s.rng.Intn(900)
	case untilReset <= 96*time.Hour:
		return 10 + s.rng.Intn(90)
	default:
		return 1 + s.rng.Intn(9)
	}
}

func parsePriority(attrs map[string]string) int {
	if attrs == nil {
		return 0
	}
	value := strings.TrimSpace(attrs["priority"])
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}

func eligibleCodexOAuthAuths(auths []*coreauth.Auth) []*coreauth.Auth {
	result := make([]*coreauth.Auth, 0, len(auths))
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
			continue
		}
		if auth.Disabled || auth.Unavailable || auth.Status != coreauth.StatusActive {
			continue
		}
		if accessTokenFromAuth(auth) == "" {
			continue
		}
		result = append(result, auth)
	}
	return result
}

func accessTokenFromAuth(auth *coreauth.Auth) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	token, _ := auth.Metadata["access_token"].(string)
	return strings.TrimSpace(token)
}

func extractCodexAccountID(auth *coreauth.Auth) (string, error) {
	if auth == nil || auth.Metadata == nil {
		return "", fmt.Errorf("missing auth metadata")
	}
	idToken, _ := auth.Metadata["id_token"].(string)
	idToken = strings.TrimSpace(idToken)
	if idToken == "" {
		return "", fmt.Errorf("missing id_token")
	}

	if claims, err := authcodex.ParseJWTToken(idToken); err == nil && claims != nil {
		if accountID := strings.TrimSpace(claims.GetAccountID()); accountID != "" {
			return accountID, nil
		}
	}

	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid id_token format")
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		// tolerate padded payloads as fallback
		payloadBytes, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return "", fmt.Errorf("decode id_token payload: %w", err)
		}
	}
	claimsMap := map[string]any{}
	if err := json.Unmarshal(payloadBytes, &claimsMap); err != nil {
		return "", fmt.Errorf("unmarshal id_token payload: %w", err)
	}
	for _, key := range []string{"chatgpt_account_id", "chatgptAccountId"} {
		if value, ok := claimsMap[key]; ok {
			if accountID := strings.TrimSpace(anyString(value)); accountID != "" {
				return accountID, nil
			}
		}
	}
	return "", fmt.Errorf("chatgpt account id claim missing")
}

func parseWHAMUsageWindow(payload map[string]any) (CodexUsageWindow, bool) {
	rateLimitMap, ok := anyMap(payload["rate_limit"])
	if !ok {
		rateLimitMap, ok = anyMap(payload["rateLimit"])
		if !ok {
			return CodexUsageWindow{}, false
		}
	}
	candidateKeys := []string{"primary_window", "secondary_window", "primaryWindow", "secondaryWindow"}
	for _, key := range candidateKeys {
		windowMap, ok := anyMap(rateLimitMap[key])
		if !ok {
			continue
		}
		window := CodexUsageWindow{}
		if value, okInt := anyInt(windowMap["limit_window_seconds"]); okInt {
			window.LimitWindowSeconds = value
		} else if value, okInt := anyInt(windowMap["limitWindowSeconds"]); okInt {
			window.LimitWindowSeconds = value
		}
		if window.LimitWindowSeconds != weeklyWindowSeconds {
			continue
		}
		if value, okFloat := anyFloat(windowMap["used_percent"]); okFloat {
			window.UsedPercent = value
		} else if value, okFloat := anyFloat(windowMap["usedPercent"]); okFloat {
			window.UsedPercent = value
		}
		if value, okInt := anyInt(windowMap["reset_after_seconds"]); okInt {
			window.ResetAfterSeconds = value
		} else if value, okInt := anyInt(windowMap["resetAfterSeconds"]); okInt {
			window.ResetAfterSeconds = value
		}
		if value, okTime := anyTime(windowMap["reset_at"]); okTime {
			window.ResetAt = &value
		} else if value, okTime := anyTime(windowMap["resetAt"]); okTime {
			window.ResetAt = &value
		}
		if window.ResetAt == nil && window.ResetAfterSeconds > 0 {
			derived := time.Now().UTC().Add(time.Duration(window.ResetAfterSeconds) * time.Second)
			window.ResetAt = &derived
		}
		if window.ResetAt == nil {
			continue
		}
		return window, true
	}
	return CodexUsageWindow{}, false
}

func anyMap(value any) (map[string]any, bool) {
	mapped, ok := value.(map[string]any)
	return mapped, ok
}

func anyString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}

func anyInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return int(parsed), true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func anyFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return parsed, true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func anyTime(value any) (time.Time, bool) {
	switch typed := value.(type) {
	case time.Time:
		if typed.IsZero() {
			return time.Time{}, false
		}
		return typed.UTC(), true
	case string:
		ts := strings.TrimSpace(typed)
		if ts == "" {
			return time.Time{}, false
		}
		parsed, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return time.Time{}, false
		}
		return parsed.UTC(), true
	default:
		return time.Time{}, false
	}
}
