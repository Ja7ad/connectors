package main

import (
	"context"
	"fmt"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/meilisearch/meilisearch-go"
	"slices"
	"strings"
	"time"
)

type driver struct{}

var _ boilerplate.Connector = &driver{}

func connect(cfg config) (meilisearch.ServiceManager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	opts := make([]meilisearch.Option, 1)
	if cfg.APIKey != "" {
		opts = append(opts, meilisearch.WithAPIKey(cfg.APIKey))
	}

	return meilisearch.New(cfg.Host, opts...), nil
}

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	cfgSchema, err := schemagen.GenerateSchema("Materialize Meilisearch Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating config schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Meilisearch Index", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-meilisearch", cfgSchema, resourceSchema)
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, newMaterialization)
}

func (d driver) Apply(ctx context.Context, apply *pm.Request_Apply) (*pm.Response_Applied, error) {
	//TODO implement me
	panic("implement me")
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open, events *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	//TODO implement me
	panic("implement me")
}

type materialization struct {
	cfg    config
	client meilisearch.ServiceManager
}

func newMaterialization(_ context.Context, _ string,
	cfg config, _ map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, property], error) {
	client, err := connect(cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to meilisearch: %w", err)
	}

	return &materialization{
		cfg:    cfg,
		client: client,
	}, nil
}

func (m *materialization) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		ConcurrentApply:     true,
		NoCreateNamespaces:  true,
		NoTruncateResources: true,
	}
}

func (m *materialization) PopulateInfoSchema(ctx context.Context, _ [][]string, is *boilerplate.InfoSchema) error {
	indexes, err := m.client.ListIndexesWithContext(ctx, &meilisearch.IndexesQuery{
		Limit:  10000,
		Offset: 0,
	})
	if err != nil {
		return err
	}

	for _, index := range indexes.Results {
		is.PushResource(index.UID)
	}

	return nil
}

func (m *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	resp, err := m.client.HealthWithContext(ctx)
	if err != nil {
		errs.Err(err)
	} else {
		if resp.Status != "available" {
			errs.Err(fmt.Errorf("meilisearch is not available, status: %s", resp.Status))
		}
	}

	return errs
}

func (m *materialization) NewConstraint(p pf.Projection, _ bool, _ fieldConfig) pm.Response_Validated_Constraint {
	var constraint pm.Response_Validated_Constraint

	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Primary key fields are required to ensure idempotent indexing"

	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "Indexing the root document is recommended for full content search"

	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "Materializing the operation type is helpful for debugging or analytics"

	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields are optional but may help with audit/debug use cases"

	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize fields that are always null"

	default:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "This field can be materialized and indexed in Meilisearch"
	}

	return constraint
}

func (m *materialization) MapType(_ boilerplate.Projection, _ fieldConfig) (property, boilerplate.ElementConverter) {
	return property{}, nil
}

func (m *materialization) Setup(_ context.Context, _ *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (m *materialization) CreateNamespace(_ context.Context, _ string) (string, error) {
	return "", nil
}

func (m *materialization) CreateResource(ctx context.Context, res boilerplate.MappedBinding[config, resource, property]) (string, boilerplate.ActionApplyFn, error) {
	idxCfg := &meilisearch.IndexConfig{
		Uid: res.ResourcePath[0],
	}

	if res.Config.PrimaryKey != "" {
		idxCfg.PrimaryKey = res.Config.PrimaryKey
	}

	taskInfo, err := m.client.CreateIndexWithContext(ctx, idxCfg)
	if err != nil {
		return "", nil, err
	}

	return idxCfg.Uid, m.applyAction(taskInfo.TaskUID), nil
}

func (m *materialization) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	indexName := resourcePath[0]

	taskInfo, err := m.client.DeleteIndexWithContext(ctx, indexName)
	if err != nil {
		return "", nil, err
	}

	return indexName, m.applyAction(taskInfo.TaskUID), nil
}

func (m *materialization) UpdateResource(ctx context.Context,
	resourcePath []string,
	_ boilerplate.ExistingResource,
	update boilerplate.BindingUpdate[config, resource, property],
) (string, boilerplate.ActionApplyFn, error) {
	if len(update.NewProjections) == 0 {
		return "", nil, nil
	}

	indexName := resourcePath[0]
	actions := make([]string, 0, len(update.NewProjections))
	fields := make(map[string]any)

	for _, np := range update.NewProjections {
		actions = append(actions,
			fmt.Sprintf("add mapping %q to index %q",
				np.Field,
				indexName,
			),
		)

		fields[np.Field] = np.Mapped
	}

	idx := m.client.Index(indexName)
	taskInfo, err := idx.UpdateDocumentsWithContext(ctx, fields, nil)
	if err != nil {
		return "", nil, err
	}

	return strings.Join(actions, "\n"), m.applyAction(taskInfo.TaskUID), nil
}

func (m *materialization) TruncateResource(_ context.Context, _ []string) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, fmt.Errorf("truncate operation is not supported by Meilisearch")
}

func (m *materialization) NewMaterializerTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, property],
	be *boilerplate.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	//TODO implement me
	panic("implement me")
}

func (m *materialization) Close(_ context.Context) {
	m.client.Close()
}

func (m *materialization) applyAction(taskUID int64) func(context.Context) error {
	return func(ctx context.Context) error {
		// Wait for the index creation task to complete
		task, err := m.client.WaitForTaskWithContext(ctx, taskUID, time.Second)
		if err != nil {
			return fmt.Errorf("waiting for index creation task: %w", err)
		}
		if task.Status != "succeeded" {
			return fmt.Errorf("index creation task failed: %s", task.Error)
		}
		return nil
	}
}
