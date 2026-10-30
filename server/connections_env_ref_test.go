package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/galaxy-io/filament"
	ingestionv1 "github.com/galaxy-io/filament/api/ingestion/v1"
	"github.com/galaxy-io/filament/datastore/sqlite"
	"github.com/galaxy-io/filament/registry"
	"github.com/galaxy-io/filament/worker"
)

type envRefSource struct{ catalogSource }

func (s *envRefSource) Spec() filament.ConnectorSpec {
	return filament.ConnectorSpec{Name: "env-ref-fixture", Config: filament.ConfigSchema{Fields: []filament.ConfigField{
		{Name: "host"},
		{Name: "password", Type: filament.FieldSecret, Required: true},
	}}}
}

// mapSecrets is a minimal in-memory Secrets provider for tests. Reads of a
// bare environment name return the process value it stands for, mirroring
// the layered provider the local CLI wires up.
type mapSecrets struct {
	values map[string]string
}

func (s *mapSecrets) Name() string { return "test" }

func (s *mapSecrets) Read(_ context.Context, ref string) (filament.Secret, error) {
	if value, ok := s.values[ref]; ok {
		return filament.Secret{Value: []byte(value)}, nil
	}
	return filament.Secret{}, filament.ErrNotFound
}

func (s *mapSecrets) Write(_ context.Context, ref string, secret filament.Secret) error {
	s.values[ref] = string(secret.Value)
	return nil
}

func (s *mapSecrets) Delete(_ context.Context, ref string) error {
	delete(s.values, ref)
	return nil
}

func TestCreateConnectionKeepsEnvironmentSecretReference(t *testing.T) {
	store := sqlite.NewMemory()
	defer store.Close()
	secrets := &mapSecrets{values: map[string]string{"FPW_ENV": "real-password"}}
	sources := registry.NewSources()
	sources.Register("env-ref-fixture", func() filament.Source { return &envRefSource{} })
	api := New(worker.Local(sources, registry.NewSinks()), store, nil, nil, WithSecrets(secrets))

	config, err := structpb.NewStruct(map[string]any{"host": "db.example", "password": "env:FPW_ENV"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := api.CreateConnection(testCtx(), connect.NewRequest(&ingestionv1.CreateConnectionRequest{
		Kind: ingestionv1.ConnectorKind_CONNECTOR_KIND_SOURCE, Name: "envtest",
		Connector: "env-ref-fixture", Config: config,
		SecretRefs: map[string]string{"password": "FPW_ENV"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.LoadConnection(testCtx(), filament.DefaultTenantID, created.Msg.GetConnection().GetId())
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.SecretRefs["password"]; got != "FPW_ENV" {
		t.Fatalf("stored secret ref = %q, want %q (the env reference was replaced by a stored copy of its text)", got, "FPW_ENV")
	}
	resolved := map[string]any{}
	if err := api.resolveConnectionSecrets(testCtx(), stored, resolved); err != nil {
		t.Fatal(err)
	}
	if got := resolved["password"]; got != "real-password" {
		t.Fatalf("resolved password = %q, want the environment value", got)
	}

	// An edit that keeps the env reference must not mint a stored copy
	// of its text either.
	connection := created.Msg.GetConnection()
	updatedConfig, err := structpb.NewStruct(map[string]any{"host": "db2.example", "password": "env:FPW_ENV"})
	if err != nil {
		t.Fatal(err)
	}
	connection.Config = updatedConfig
	if _, err := api.UpdateConnection(testCtx(), connect.NewRequest(&ingestionv1.UpdateConnectionRequest{Connection: connection})); err != nil {
		t.Fatal(err)
	}
	restored, err := store.LoadConnection(testCtx(), filament.DefaultTenantID, created.Msg.GetConnection().GetId())
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.SecretRefs["password"]; got != "FPW_ENV" {
		t.Fatalf("after update, stored secret ref = %q, want %q", got, "FPW_ENV")
	}
}
