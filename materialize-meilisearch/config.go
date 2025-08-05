package main

import (
	"fmt"
	"net/url"
)

type config struct {
	Host       string `json:"host" jsonschema:"title=Host" jsonschema_description:"The full URL of the Meilisearch server. Example: http://localhost:7700" jsonschema_extras:"order=0"`
	APIKey     string `json:"apiKey,omitempty" jsonschema:"title=API Key" jsonschema_description:"Optional API key used to authenticate with Meilisearch. Leave empty if not required." jsonschema_extras:"order=1"`
	HardDelete bool   `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=2"`
}

func (c config) FeatureFlags() (raw string, defaults map[string]bool) {
	return "", map[string]bool{}
}

func (c config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("missing 'host'")
	}

	parsed, err := url.Parse(c.Host)
	if err != nil {
		return fmt.Errorf("invalid 'host' URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("unsupported URL scheme '%s': must be http or https", parsed.Scheme)
	}
	if parsed.Host == "" {
		return fmt.Errorf("invalid 'host': missing hostname")
	}

	return nil
}

type resource struct {
	IndexName  string `json:"index" jsonschema:"title=Index name" jsonschema_extras:"x-collection-name=true"`
	PrimaryKey string `json:"primaryKey,omitempty" jsonschema:"title=Primary Key,description=The field used as the primary key in this Meilisearch index (must exist in each document)."`
}

func (r resource) Validate() error {
	if r.IndexName == "" {
		return fmt.Errorf("index is required")
	}
	if r.PrimaryKey == "" {
		return fmt.Errorf("primaryKey is required")
	}
	return nil
}

func (r resource) WithDefaults(_ config) resource {
	return r
}

func (r resource) Parameters() ([]string, bool, error) {
	return []string{r.IndexName, r.PrimaryKey}, false, nil
}
