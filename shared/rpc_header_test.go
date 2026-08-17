package shared

import (
	"os"
	"testing"
)

func TestResolveRPCHeader_EnvOverridesConfig(t *testing.T) {
	t.Setenv("RPC_HEADER_KEY", "X-Auth-Token")
	t.Setenv("RPC_HEADER_VALUE", "from-env")
	Options.RpcHeaderKey = "X-Auth-Token"
	Options.RpcHeaderValue = "from-config"

	key, val, source := resolveRPCHeader()
	if key != "X-Auth-Token" || val != "from-env" || source != "environment" {
		t.Fatalf("got key=%q val=%q source=%q", key, val, source)
	}
}

func TestResolveRPCHeader_ConfigFallback(t *testing.T) {
	os.Unsetenv("RPC_HEADER_KEY")
	os.Unsetenv("RPC_HEADER_VALUE")
	Options.RpcHeaderKey = "X-Auth-Token"
	Options.RpcHeaderValue = "from-config"

	key, val, source := resolveRPCHeader()
	if key != "X-Auth-Token" || val != "from-config" || source != "config" {
		t.Fatalf("got key=%q val=%q source=%q", key, val, source)
	}
}

func TestResolveRPCHeader_Empty(t *testing.T) {
	os.Unsetenv("RPC_HEADER_KEY")
	os.Unsetenv("RPC_HEADER_VALUE")
	Options.RpcHeaderKey = ""
	Options.RpcHeaderValue = ""

	key, val, source := resolveRPCHeader()
	if key != "" || val != "" || source != "" {
		t.Fatalf("got key=%q val=%q source=%q", key, val, source)
	}
}
