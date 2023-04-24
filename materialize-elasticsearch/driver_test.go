package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestConfigUnmarshalling(t *testing.T) {
	var input = `{
		"endpoint": "http://foo.test", 
		"credentials": {
			"username": "user",
			"password": "pass"
		}
	}`
	var out config
	require.NoError(t, pf.UnmarshalStrict(json.RawMessage([]byte(input)), &out))
	require.Equal(t, "http://foo.test", out.Endpoint)
	require.Equal(t, "user", out.Credentials.Username)
	require.Equal(t, "pass", out.Credentials.Password)

	//
	input = `{"endpoint": "http://foo.test"}`
	out = config{}
	require.NoError(t, pf.UnmarshalStrict(json.RawMessage([]byte(input)), &out))
	require.Equal(t, "http://foo.test", out.Endpoint)
}

func TestConfigValidation(t *testing.T) {
	var validConfig = config{
		Endpoint: "testEndpoint",
		Credentials: credentials{
			Username: "testUsername",
			Password: "testPassword",
		},
	}

	require.NoError(t, validConfig.Validate())

	var missingEndpoint = validConfig
	missingEndpoint.Endpoint = ""
	require.Error(t, missingEndpoint.Validate(), "expected validation error")

	var userWithoutPassword = validConfig
	userWithoutPassword.Credentials.Password = ""

	require.Error(t, userWithoutPassword.Validate(), "expected validation error")
}

func TestResource(t *testing.T) {
	var validResourceA resource
	pf.UnmarshalStrict(json.RawMessage(`{
		"index":        "testIndex",
		"delta_updates": true,
		"number_of_shards": 1
	}`), &validResourceA)
	require.NoError(t, validResourceA.Validate())
	require.Equal(t, 1, validResourceA.NumOfShards)
	require.Equal(t, 0, validResourceA.NumOfReplicas)

	var missingIndex = validResourceA
	missingIndex.Index = ""
	require.Error(t, missingIndex.Validate(), "expected validation error")

	var invalidShards = validResourceA
	invalidShards.NumOfShards = 0
	require.Error(t, invalidShards.Validate(), "expected validation error")

	var validResourceB resource
	pf.UnmarshalStrict(json.RawMessage(`{
		"index":        "testIndex",
		"delta_updates": true,
		"number_of_shards": 3,
		"number_of_replicas": 4
	}`), &validResourceB)
	require.NoError(t, validResourceB.Validate())
	require.Equal(t, 3, validResourceB.NumOfShards)
	require.Equal(t, 4, validResourceB.NumOfReplicas)
}

func TestDriverSpec(t *testing.T) {
	var drv = driver{}
	var resp, err1 = drv.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err1)
	var formatted, err2 = json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err2)
	cupaloy.SnapshotT(t, formatted)
}
