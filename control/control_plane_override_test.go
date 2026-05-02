package control

import (
	"io"
	"testing"
	"time"

	"github.com/daeuniverse/dae/config"
	"github.com/sirupsen/logrus"
)

func TestParseGroupOverrideOptionSwitchStabilityOverrides(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	base := config.Global{
		SwitchCooldown: 10 * time.Second,
		SwitchMinWins:  1,
	}
	group := config.Group{
		SwitchCooldown: 45 * time.Second,
		SwitchMinWins:  3,
	}

	option, err := ParseGroupOverrideOption(group, base, logger)
	if err != nil {
		t.Fatalf("ParseGroupOverrideOption() error = %v", err)
	}
	if option == nil {
		t.Fatal("ParseGroupOverrideOption() = nil, want override option")
	}
	if option.SwitchCooldown != 45*time.Second {
		t.Fatalf("SwitchCooldown = %v, want %v", option.SwitchCooldown, 45*time.Second)
	}
	if option.SwitchMinWins != 3 {
		t.Fatalf("SwitchMinWins = %d, want 3", option.SwitchMinWins)
	}
}

func TestParseGroupOverrideOptionSwitchStabilityZeroValuesPreserveBase(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	base := config.Global{
		SwitchCooldown: 10 * time.Second,
		SwitchMinWins:  1,
	}
	group := config.Group{
		CheckInterval: time.Minute,
	}

	option, err := ParseGroupOverrideOption(group, base, logger)
	if err != nil {
		t.Fatalf("ParseGroupOverrideOption() error = %v", err)
	}
	if option == nil {
		t.Fatal("ParseGroupOverrideOption() = nil, want override option")
	}
	if option.SwitchCooldown != 10*time.Second {
		t.Fatalf("SwitchCooldown = %v, want %v", option.SwitchCooldown, 10*time.Second)
	}
	if option.SwitchMinWins != 1 {
		t.Fatalf("SwitchMinWins = %d, want 1", option.SwitchMinWins)
	}
}
