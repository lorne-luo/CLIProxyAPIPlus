package config

import "testing"

func TestOpenAICompatibility_IsEnabled(t *testing.T) {
	t.Run("nil defaults to true", func(t *testing.T) {
		o := &OpenAICompatibility{}
		if !o.IsEnabled() {
			t.Error("expected IsEnabled() to return true when Enabled is nil")
		}
	})

	t.Run("explicit true", func(t *testing.T) {
		enabled := true
		o := &OpenAICompatibility{Enabled: &enabled}
		if !o.IsEnabled() {
			t.Error("expected IsEnabled() to return true when Enabled is true")
		}
	})

	t.Run("explicit false", func(t *testing.T) {
		enabled := false
		o := &OpenAICompatibility{Enabled: &enabled}
		if o.IsEnabled() {
			t.Error("expected IsEnabled() to return false when Enabled is false")
		}
	})
}
