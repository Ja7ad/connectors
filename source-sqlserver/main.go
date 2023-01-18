package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"

	networkTunnel "github.com/estuary/connectors/go-network-tunnel"
	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

func main() {
	boilerplate.RunMain(sqlserverDriver)
}

var sqlserverDriver = &sqlcapture.Driver{
	ConfigSchema:     configSchema(),
	DocumentationURL: "https://go.estuary.dev/source-sqlserver",
	Connect:          connectSQLServer,
}

const defaultPort = "1433"

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address  string `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User     string `json:"user" jsonschema:"default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password string `json:"password" jsonschema:"description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	///Database string         `json:"database" jsonschema:"default=postgres,description=Logical database name to capture from." jsonschema_extras:"order=3"`
	Advanced      advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
	NetworkTunnel *tunnelConfig  `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	//PublicationName   string `json:"publicationName,omitempty" jsonschema:"default=flow_publication,description=The name of the PostgreSQL publication to replicate from."`
	//SlotName          string `json:"slotName,omitempty" jsonschema:"default=flow_slot,description=The name of the PostgreSQL replication slot to replicate from."`
	WatermarksTable string `json:"watermarksTable,omitempty" jsonschema:"default=dbo.flow_watermarks,description=The name of the table used for watermark writes during backfills. Must be fully-qualified in '<schema>.<table>' form."`
	//SkipBackfills     string `json:"skip_backfills,omitempty" jsonschema:"title=Skip Backfills,description=A comma-separated list of fully-qualified table names which should not be backfilled."`
	BackfillChunkSize int `json:"backfill_chunk_size,omitempty" jsonschema:"title=Backfill Chunk Size,default=4096,description=The number of rows which should be fetched from the database in a single backfill query."`
}

type tunnelConfig struct {
	SSHForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

type sshForwarding struct {
	SSHEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Advanced.WatermarksTable != "" && !strings.Contains(c.Advanced.WatermarksTable, ".") {
		return fmt.Errorf("invalid 'watermarksTable' configuration: table name %q must be fully-qualified as \"<schema>.<table>\"", c.Advanced.WatermarksTable)
	}
	//if c.Advanced.SkipBackfills != "" {
	//	for _, skipStreamID := range strings.Split(c.Advanced.SkipBackfills, ",") {
	//		if !strings.Contains(skipStreamID, ".") {
	//			return fmt.Errorf("invalid 'skipBackfills' configuration: table name %q must be fully-qualified as \"<schema>.<table>\"", skipStreamID)
	//		}
	//	}
	//}
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	// Note these are 1:1 with 'omitempty' in Config field tags,
	// which cause these fields to be emitted as non-required.
	// if c.Advanced.SlotName == "" {
	// 	c.Advanced.SlotName = "flow_slot"
	// }
	// if c.Advanced.PublicationName == "" {
	// 	c.Advanced.PublicationName = "flow_publication"
	// }
	if c.Advanced.WatermarksTable == "" {
		c.Advanced.WatermarksTable = "dbo.flow_watermarks"
	}
	if c.Advanced.BackfillChunkSize <= 0 {
		c.Advanced.BackfillChunkSize = 4096
	}

	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the MS SQL
	// default of 1433.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":" + defaultPort
	}
}

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI() string {
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost
	// to the target via the bastion server, so we use the tunnel's address.
	var address = c.Address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SSHForwarding != nil && c.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		address = "localhost:" + defaultPort
	}

	var params = make(url.Values)
	params.Add("app name", "Flow CDC Connector")
	params.Add("encrypt", "true")
	params.Add("TrustServerCertificate", "true")
	var connectURL = &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(c.User, c.Password),
		Host:     address,
		RawQuery: params.Encode(),
	}
	return connectURL.String()
}

func configSchema() json.RawMessage {
	var schema = schemagen.GenerateSchema("PostgreSQL Connection", &Config{})
	var configSchema, err = schema.MarshalJSON()
	if err != nil {
		panic(err)
	}
	return json.RawMessage(configSchema)
}

func connectSQLServer(ctx context.Context, name string, cfg json.RawMessage) (sqlcapture.Database, error) {
	var config Config
	if err := pf.UnmarshalStrict(cfg, &config); err != nil {
		return nil, fmt.Errorf("error parsing config json: %w", err)
	}
	config.SetDefaults()

	// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
	if config.NetworkTunnel != nil && config.NetworkTunnel.SSHForwarding != nil && config.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		host, port, err := net.SplitHostPort(config.Address)
		if err != nil {
			return nil, fmt.Errorf("splitting address to host and port: %w", err)
		}

		var sshConfig = &networkTunnel.SshConfig{
			SshEndpoint: config.NetworkTunnel.SSHForwarding.SSHEndpoint,
			PrivateKey:  []byte(config.NetworkTunnel.SSHForwarding.PrivateKey),
			ForwardHost: host,
			ForwardPort: port,
			LocalPort:   defaultPort,
		}
		var tunnel = sshConfig.CreateTunnel()

		// FIXME/question: do we need to shut down the tunnel manually if it is a child process?
		// at the moment tunnel.Stop is not being called anywhere, but if the connector shuts down, the child process also shuts down.
		if err := tunnel.Start(); err != nil {
			log.WithField("error", err).Error("network tunnel error")
		}
	}

	var db = &sqlserverDatabase{config: &config}
	if err := db.connect(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

type sqlserverDatabase struct {
	config *Config
	conn   *sql.DB
}

func (db *sqlserverDatabase) connect(ctx context.Context) error {
	log.WithFields(log.Fields{
		"address": db.config.Address,
		"user":    db.config.User,
	}).Info("initializing connector")

	var conn, err = sql.Open("sqlserver", db.config.ToURI())
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	db.conn = conn
	return nil
}

// Close shuts down the database connection.
func (db *sqlserverDatabase) Close(ctx context.Context) error {
	if err := db.conn.Close(); err != nil {
		return fmt.Errorf("error closing database connection: %w", err)
	}
	return nil
}

// ReplicationStream constructs a new ReplicationStream object, from which
// a neverending sequence of change events can be read.
func (db *sqlserverDatabase) ReplicationStream(ctx context.Context, startCursor string) (sqlcapture.ReplicationStream, error) {
	panic("NOT YET IMPLEMENTED: ReplicationStream")
}

// WriteWatermark writes the provided string into the 'watermarks' table.
func (db *sqlserverDatabase) WriteWatermark(ctx context.Context, watermark string) error {
	panic("NOT YET IMPLEMENTED: WriteWatermark")
}

// WatermarksTable returns the name of the table to which WriteWatermarks writes UUIDs.
func (db *sqlserverDatabase) WatermarksTable() string {
	return db.config.Advanced.WatermarksTable
}

// ScanTableChunk fetches a chunk of rows from the specified table, resuming from the `resumeAfter` row key if non-nil.
func (db *sqlserverDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, keyColumns []string, resumeAfter []byte) ([]*sqlcapture.ChangeEvent, error) {
	panic("NOT YET IMPLEMENTED: ScanTableChunk")
}

// Returns an empty instance of the source-specific metadata (used for JSON schema generation).
func (db *sqlserverDatabase) EmptySourceMetadata() sqlcapture.SourceMetadata {
	return &sqlserverSourceInfo{}
}

// sqlserverSourceInfo is source metadata for data capture events.
type sqlserverSourceInfo struct {
	sqlcapture.SourceCommon
}

func (s *sqlserverSourceInfo) Common() sqlcapture.SourceCommon {
	return s.SourceCommon
}

func (s *sqlserverSourceInfo) Cursor() string {
	return "TBD"
}

// ShouldBackfill returns true if a given table's contents should be backfilled.
func (db *sqlserverDatabase) ShouldBackfill(streamID string) bool {
	panic("NOT YET IMPLEMENTED: ShouldBackfill")
}
