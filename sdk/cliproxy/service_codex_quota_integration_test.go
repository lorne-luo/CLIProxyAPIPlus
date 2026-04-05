package cliproxy

import (
	"context"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestStartCodexQuotaService_NotStartedWithoutCodexOAuth(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	ctx := context.Background()

	service.startCodexQuotaService(ctx)
	if service.codexQuotaService != nil {
		t.Fatal("expected codex quota service to remain nil without codex oauth auth")
	}
}

func TestStartCodexQuotaService_StartedWithCodexOAuth(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	ctx := context.Background()

	_, err := service.coreManager.Register(ctx, &coreauth.Auth{
		ID:       "codex-oauth-1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"access_token": "token-1"},
	})
	if err != nil {
		t.Fatalf("failed to register auth: %v", err)
	}

	service.startCodexQuotaService(ctx)
	if service.codexQuotaService == nil {
		t.Fatal("expected codex quota service to start")
	}
	service.stopCodexQuotaService()
}

func TestApplyCoreAuthAddOrUpdate_StartsCodexQuotaService(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	ctx := context.Background()

	service.applyCoreAuthAddOrUpdate(ctx, &coreauth.Auth{
		ID:       "codex-oauth-2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"access_token": "token-2"},
	})
	if service.codexQuotaService == nil {
		t.Fatal("expected codex quota service to start from auth update")
	}
	service.stopCodexQuotaService()
}

func TestShutdown_StopsCodexQuotaService(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	ctx := context.Background()

	_, err := service.coreManager.Register(ctx, &coreauth.Auth{
		ID:       "codex-oauth-3",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"access_token": "token-3"},
	})
	if err != nil {
		t.Fatalf("failed to register auth: %v", err)
	}

	service.startCodexQuotaService(ctx)
	if service.codexQuotaService == nil {
		t.Fatal("expected codex quota service to start")
	}

	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
	if service.codexQuotaService != nil {
		t.Fatal("expected codex quota service to be stopped and cleared")
	}
}
