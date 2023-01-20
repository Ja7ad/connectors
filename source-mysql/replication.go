package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
	"vitess.io/vitess/go/vt/sqlparser"
)

// replicationBufferSize controls how many change events can be buffered in the
// replicationStream before it stops receiving further events from MySQL.
// In normal use it's a constant, it's just a variable so that tests are more
// likely to exercise blocking sends and backpressure.
var replicationBufferSize = 1024

func (db *mysqlDatabase) ReplicationStream(ctx context.Context, startCursor string) (sqlcapture.ReplicationStream, error) {
	var address = db.config.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:5432
	// to address through the bastion server, so we use the tunnel's address
	if db.config.NetworkTunnel != nil && db.config.NetworkTunnel.SSHForwarding != nil && db.config.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		address = "localhost:3306"
	}
	var host, port, err = splitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid mysql address: %w", err)
	}

	var pos mysql.Position
	if startCursor != "" {
		var binlogName, binlogPos, err = splitCursor(startCursor)
		if err != nil {
			return nil, fmt.Errorf("invalid resume cursor: %w", err)
		}
		pos.Name = binlogName
		pos.Pos = uint32(binlogPos)
	} else {
		// Get the initial binlog cursor from the source database's latest binlog position
		var results, err = db.conn.Execute("SHOW MASTER STATUS;")
		if err != nil {
			return nil, fmt.Errorf("error getting latest binlog position: %w", err)
		}
		if len(results.Values) == 0 {
			return nil, fmt.Errorf("failed to query latest binlog position (is binary logging enabled on %q?)", host)
		}
		var row = results.Values[0]
		pos.Name = string(row[0].AsString())
		pos.Pos = uint32(row[1].AsInt64())
		logrus.WithField("pos", pos).Debug("initialized binlog position")
	}

	var syncConfig = replication.BinlogSyncerConfig{
		ServerID: uint32(db.config.Advanced.NodeID),
		Flavor:   "mysql", // TODO(wgd): See what happens if we change this and run against MariaDB?
		Host:     host,
		Port:     uint16(port),
		User:     db.config.User,
		Password: db.config.Password,
		// TODO(wgd): Maybe add 'serverName' checking as described over in Connect()
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
		// Request that timestamp values coming via replication be interpreted as UTC.
		TimestampStringLocation: time.UTC,
	}

	logrus.WithFields(logrus.Fields{"pos": pos}).Info("starting replication")
	var streamer *replication.BinlogStreamer
	var syncer = replication.NewBinlogSyncer(syncConfig)
	if streamer, err = syncer.StartSync(pos); err == nil {
		logrus.Debug("replication connected with TLS")
	} else {
		syncer.Close()
		syncConfig.TLSConfig = nil
		syncer = replication.NewBinlogSyncer(syncConfig)
		if streamer, err = syncer.StartSync(pos); err == nil {
			logrus.Warn("replication connected without TLS")
		} else {
			return nil, fmt.Errorf("error starting binlog sync: %w", err)
		}
	}

	var stream = &mysqlReplicationStream{
		db:       db,
		syncer:   syncer,
		streamer: streamer,
		cursor:   pos,
	}
	stream.tables.active = make(map[string]struct{})
	stream.tables.discovery = make(map[string]sqlcapture.DiscoveryInfo)
	stream.tables.metadata = make(map[string]*mysqlTableMetadata)
	stream.tables.keyColumns = make(map[string][]string)
	return stream, nil
}

func splitCursor(cursor string) (string, int64, error) {
	seps := strings.Split(cursor, ":")
	if len(seps) != 2 {
		return "", 0, fmt.Errorf("input %q must have <logfile>:<position> shape", cursor)
	}
	position, err := strconv.ParseInt(seps[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid position value %q: %w", seps[1], err)
	}
	return seps[0], position, nil
}

func splitHostPort(addr string) (string, int64, error) {
	seps := strings.Split(addr, ":")
	if len(seps) != 2 {
		return "", 0, fmt.Errorf("input %q must have <host>:<port> shape", addr)
	}
	port, err := strconv.ParseInt(seps[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port number %q: %w", seps[1], err)
	}
	return seps[0], port, nil
}

type mysqlReplicationStream struct {
	db       *mysqlDatabase
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
	cursor   mysql.Position
	events   chan sqlcapture.DatabaseEvent // Output channel from replication worker goroutine

	cancel context.CancelFunc // Cancellation thunk for the replication worker goroutine
	errCh  chan error         // Error output channel for the replication worker goroutine

	gtidTimestamp time.Time // The OriginalCommitTimestamp value of the last GTID Event

	// The active tables set and associated metadata, guarded by a
	// mutex so it can be modified from the main goroutine while it's
	// read from the replication goroutine.
	tables struct {
		sync.RWMutex
		active        map[string]struct{}
		discovery     map[string]sqlcapture.DiscoveryInfo
		metadata      map[string]*mysqlTableMetadata
		keyColumns    map[string][]string
		dirtyMetadata []string
	}
}

type mysqlTableMetadata struct {
	Schema mysqlTableSchema `json:"schema"`
}

type mysqlTableSchema struct {
	Columns     []string               `json:"columns"`
	ColumnTypes map[string]interface{} `json:"types"`
}

func (rs *mysqlReplicationStream) StartReplication(ctx context.Context) error {
	if rs.cancel != nil {
		return fmt.Errorf("internal error: replication stream already started")
	}

	var streamCtx, streamCancel = context.WithCancel(ctx)
	rs.events = make(chan sqlcapture.DatabaseEvent, replicationBufferSize)
	rs.errCh = make(chan error)
	rs.cancel = streamCancel

	go func() {
		var err = rs.run(streamCtx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		rs.syncer.Close()
		close(rs.events)
		rs.errCh <- err
	}()
	return nil
}

func (rs *mysqlReplicationStream) run(ctx context.Context) error {
	for {
		// Send "Metadata Change" events to the consumer where applicable.
		//
		// Note that the work is divided in two here so that the mutex-acquiring
		// part cannot block and the blocking send doesn't hold the mutex. This
		// helps avoid a (very unlikely) deadlock with the main thread calling
		// ActivateTable() while the events buffer is full.
		var metadataEvents []sqlcapture.DatabaseEvent
		rs.tables.RLock()
		for _, streamID := range rs.tables.dirtyMetadata {
			var bs, err = json.Marshal(rs.tables.metadata[streamID])
			if err != nil {
				return fmt.Errorf("error serializing metadata JSON for %q: %w", streamID, err)
			}
			metadataEvents = append(metadataEvents, &sqlcapture.MetadataEvent{
				StreamID: streamID,
				Metadata: json.RawMessage(bs),
			})
		}
		rs.tables.dirtyMetadata = nil
		rs.tables.RUnlock()
		for _, metadataEvent := range metadataEvents {
			rs.events <- metadataEvent
		}

		// Process the next binlog event from the database.
		var event, err = rs.streamer.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("error getting next event: %w", err)
		}

		if event.Header.LogPos > 0 {
			rs.cursor.Pos = event.Header.LogPos
		}

		switch data := event.Event.(type) {
		case *replication.RowsEvent:
			var schema, table = string(data.Table.Schema), string(data.Table.Table)
			var streamID = sqlcapture.JoinStreamID(schema, table)
			// Skip change events from tables which aren't being captured
			if !rs.tableActive(streamID) {
				continue
			}

			var sourceMeta = &mysqlSourceInfo{
				SourceCommon: sqlcapture.SourceCommon{
					Schema: schema,
					Table:  table,
				},
			}
			if !rs.gtidTimestamp.IsZero() {
				sourceMeta.SourceCommon.Millis = rs.gtidTimestamp.UnixMilli()
			}

			// Get column names and types from persistent metadata. If available, allow
			// override the persistent column name tracking using binlog row metadata.
			var metadata, ok = rs.tableMetadata(streamID)
			if !ok || metadata == nil {
				return fmt.Errorf("missing metadata for stream %q", streamID)
			}
			var columnTypes = metadata.Schema.ColumnTypes
			var columnNames = data.Table.ColumnNameString()
			if len(columnNames) == 0 {
				columnNames = metadata.Schema.Columns
			}

			keyColumns, ok := rs.keyColumns(streamID)
			if !ok {
				return fmt.Errorf("unknown key columns for stream %q", streamID)
			}

			switch event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				for _, row := range data.Rows {
					var after, err = decodeRow(streamID, columnNames, row)
					if err != nil {
						return fmt.Errorf("error decoding row values: %w", err)
					}
					rowKey, err := sqlcapture.EncodeRowKey(keyColumns, after, columnTypes, encodeKeyFDB)
					if err != nil {
						return fmt.Errorf("error encoding row key for %q: %w", streamID, err)
					}
					if err := rs.db.translateRecordFields(columnTypes, after); err != nil {
						return fmt.Errorf("error translating 'after' of %q InsertOp: %w", streamID, err)
					}
					rs.events <- &sqlcapture.ChangeEvent{
						Operation: sqlcapture.InsertOp,
						RowKey:    rowKey,
						Source:    sourceMeta,
						After:     after,
					}
				}
			case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				for rowIdx := range data.Rows {
					// Update events contain alternating (before, after) pairs of rows
					if rowIdx%2 == 1 {
						before, err := decodeRow(streamID, columnNames, data.Rows[rowIdx-1])
						if err != nil {
							return fmt.Errorf("error decoding row values: %w", err)
						}
						after, err := decodeRow(streamID, columnNames, data.Rows[rowIdx])
						if err != nil {
							return fmt.Errorf("error decoding row values: %w", err)
						}
						rowKey, err := sqlcapture.EncodeRowKey(keyColumns, after, columnTypes, encodeKeyFDB)
						if err != nil {
							return fmt.Errorf("error encoding row key for %q: %w", streamID, err)
						}
						if err := rs.db.translateRecordFields(columnTypes, before); err != nil {
							return fmt.Errorf("error translating 'before' of %q UpdateOp: %w", streamID, err)
						}
						if err := rs.db.translateRecordFields(columnTypes, after); err != nil {
							return fmt.Errorf("error translating 'after' of %q UpdateOp: %w", streamID, err)
						}
						rs.events <- &sqlcapture.ChangeEvent{
							Operation: sqlcapture.UpdateOp,
							RowKey:    rowKey,
							Source:    sourceMeta,
							Before:    before,
							After:     after,
						}
					}
				}
			case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				for _, row := range data.Rows {
					var before, err = decodeRow(streamID, columnNames, row)
					if err != nil {
						return fmt.Errorf("error decoding row values: %w", err)
					}
					rowKey, err := sqlcapture.EncodeRowKey(keyColumns, before, columnTypes, encodeKeyFDB)
					if err != nil {
						return fmt.Errorf("error encoding row key for %q: %w", streamID, err)
					}
					if err := rs.db.translateRecordFields(columnTypes, before); err != nil {
						return fmt.Errorf("error translating 'before' of %q DeleteOp: %w", streamID, err)
					}
					rs.events <- &sqlcapture.ChangeEvent{
						Operation: sqlcapture.DeleteOp,
						RowKey:    rowKey,
						Source:    sourceMeta,
						Before:    before,
					}
				}
			default:
				return fmt.Errorf("unknown row event type: %q", event.Header.EventType)
			}
		case *replication.XIDEvent:
			logrus.WithFields(logrus.Fields{
				"xid":    data.XID,
				"cursor": rs.cursor,
			}).Trace("XID Event")
			rs.events <- &sqlcapture.FlushEvent{
				Source: &mysqlSourceInfo{
					FlushCursor: fmt.Sprintf("%s:%d", rs.cursor.Name, rs.cursor.Pos),
				},
			}
		case *replication.TableMapEvent:
			logrus.WithField("data", data).Trace("Table Map Event")
		case *replication.GTIDEvent:
			logrus.WithField("data", data).Trace("GTID Event")
			rs.gtidTimestamp = data.OriginalCommitTime()
		case *replication.PreviousGTIDsEvent:
			logrus.WithField("gtids", data.GTIDSets).Trace("PreviousGTIDs Event")
		case *replication.QueryEvent:
			if err := rs.handleQuery(string(data.Schema), string(data.Query)); err != nil {
				return fmt.Errorf("error processing query event: %w", err)
			}
		case *replication.RotateEvent:
			rs.cursor.Name = string(data.NextLogName)
			rs.cursor.Pos = uint32(data.Position)
			logrus.WithFields(logrus.Fields{
				"name": rs.cursor.Name,
				"pos":  rs.cursor.Pos,
			}).Trace("Rotate Event")
		case *replication.FormatDescriptionEvent:
			logrus.WithField("data", data).Trace("Format Description Event")
		case *replication.GenericEvent:
			logrus.WithField("event", event.Header.EventType.String()).Debug("Generic Event")
		default:
			return fmt.Errorf("unhandled event type: %q", event.Header.EventType)
		}
	}
}

func decodeRow(streamID string, colNames []string, row []interface{}) (map[string]interface{}, error) {
	// If we have more or fewer values than expected, something has gone wrong
	// with our metadata tracking and it's best to die immediately. The fix in
	// this case is almost always going to be deleting and recreating the
	// capture binding for a particular table.
	if len(row) != len(colNames) {
		if len(colNames) == 0 {
			return nil, fmt.Errorf("metadata error (go.estuary.dev/eiKbOh): unknown column names for stream %q", streamID)
		}
		return nil, fmt.Errorf("metadata error (go.estuary.dev/eiKbOh): change event on stream %q contains %d values, expected %d", streamID, len(row), len(colNames))
	}

	var fields = make(map[string]interface{})
	for idx, val := range row {
		fields[colNames[idx]] = val
	}
	return fields, nil
}

// Query Events in the MySQL binlog are normalized enough that we can use
// prefix matching to detect many types of query that we just completely
// don't care about. This is good, because the Vitess SQL parser disagrees
// with the binlog Query Events for some statements like GRANT and CREATE USER.
// TODO(johnny): SET STATEMENT is not safe in the general case, and we want to re-visit
// by extracting and ignoring a SET STATEMENT stanza prior to parsing.
var ignoreQueriesRe = regexp.MustCompile(`(?i)^(BEGIN|COMMIT|GRANT|CREATE USER|CREATE DEFINER|DROP USER|DROP PROCEDURE|SET STATEMENT|# )`)

func (rs *mysqlReplicationStream) handleQuery(schema, query string) error {
	// There are basically three types of query events we might receive:
	//   * An INSERT/UPDATE/DELETE query is an error, we should never receive
	//     these if the server's `binlog_format` is set to ROW as it should be
	//     for CDC to work properly.
	//   * Various DDL queries like CREATE/ALTER/DROP/TRUNCATE/RENAME TABLE,
	//     which should in general be treated like errors *if they occur on
	//     a table we're capturing*, though we expect to eventually handle
	//     some subset of possible alterations like adding/renaming columns.
	//   * Some other queries like BEGIN and CREATE DATABASE and other things
	//     that we don't care about, either because they change things that
	//     don't impact our capture or because we get the relevant information
	//     by some other means.
	if ignoreQueriesRe.MatchString(query) {
		logrus.WithField("query", query).Debug("ignoring query event")
		return nil
	}
	logrus.WithField("query", query).Debug("handling query event")

	var stmt, err = sqlparser.Parse(query)
	if err != nil {
		return fmt.Errorf("parsing query %s: %w", query, err)
	}
	logrus.WithField("stmt", fmt.Sprintf("%#v", stmt)).Debug("parsed query")

	switch stmt := stmt.(type) {
	case *sqlparser.CreateDatabase:
	case *sqlparser.CreateTable:
		logrus.WithField("query", query).Trace("ignoring benign query")
	case *sqlparser.AlterTable:
		if streamID := resolveTableName(schema, stmt.Table); rs.tableActive(streamID) {
			logrus.WithFields(logrus.Fields{
				"query":         query,
				"partitionSpec": stmt.PartitionSpec,
				"alterOptions":  stmt.AlterOptions,
			}).Info("parsed components of ALTER TABLE statement")

			if stmt.PartitionSpec == nil || len(stmt.AlterOptions) != 0 {
				return fmt.Errorf("unsupported operation (go.estuary.dev/eVVwet): %s", query)
			}
		}
	case *sqlparser.DropTable:
		for _, table := range stmt.FromTables {
			if streamID := resolveTableName(schema, table); rs.tableActive(streamID) {
				return fmt.Errorf("unsupported operation (go.estuary.dev/eVVwet): %s", query)
			}
		}
	case *sqlparser.TruncateTable:
		if streamID := resolveTableName(schema, stmt.Table); rs.tableActive(streamID) {
			return fmt.Errorf("unsupported operation (go.estuary.dev/eVVwet): %s", query)
		}
	case *sqlparser.RenameTable:
		for _, pair := range stmt.TablePairs {
			if streamID := resolveTableName(schema, pair.FromTable); rs.tableActive(streamID) {
				return fmt.Errorf("operation conflicts with %s (go.estuary.dev/eVVwet): %s", streamID, query)
			}
			if streamID := resolveTableName(schema, pair.ToTable); rs.tableActive(streamID) {
				return fmt.Errorf("operation conflicts with %s (go.estuary.dev/eVVwet): %s", streamID, query)
			}
		}
	case *sqlparser.Insert:
		if streamID := resolveTableName(schema, stmt.Table); rs.tableActive(streamID) {
			return fmt.Errorf("unsupported DML query (go.estuary.dev/IK5EVx): %s", query)
		}
	case *sqlparser.Update:
		// TODO(wgd): It would be nice to only halt on UPDATE statements impacting
		// active tables. Unfortunately UPDATE queries are complicated and it's not
		// as simple to implement that check as for INSERT and DELETE.
		return fmt.Errorf("unsupported DML query (go.estuary.dev/IK5EVx): %s", query)
	case *sqlparser.Delete:
		for _, target := range stmt.Targets {
			if streamID := resolveTableName(schema, target); rs.tableActive(streamID) {
				return fmt.Errorf("unsupported DML query (go.estuary.dev/IK5EVx): %s", query)
			}
		}
	case *sqlparser.OtherAdmin:
		// We ignore queries like REPAIR or OPTIMIZE.
	default:
		return fmt.Errorf("unhandled query (go.estuary.dev/ceqr74): %s", query)
	}

	return nil
}

func resolveTableName(defaultSchema string, name sqlparser.TableName) string {
	var schema, table = name.Qualifier.String(), name.Name.String()
	if schema == "" {
		schema = defaultSchema
	}
	return sqlcapture.JoinStreamID(schema, table)
}

func (rs *mysqlReplicationStream) tableMetadata(streamID string) (*mysqlTableMetadata, bool) {
	rs.tables.RLock()
	defer rs.tables.RUnlock()
	var meta, ok = rs.tables.metadata[streamID]
	return meta, ok
}

func (rs *mysqlReplicationStream) tableActive(streamID string) bool {
	rs.tables.RLock()
	defer rs.tables.RUnlock()
	var _, ok = rs.tables.active[streamID]
	return ok
}

func (rs *mysqlReplicationStream) keyColumns(streamID string) ([]string, bool) {
	rs.tables.RLock()
	defer rs.tables.RUnlock()
	var keyColumns, ok = rs.tables.keyColumns[streamID]
	return keyColumns, ok
}

func (rs *mysqlReplicationStream) ActivateTable(streamID string, keyColumns []string, discovery *sqlcapture.DiscoveryInfo, metadataJSON json.RawMessage) error {
	rs.tables.Lock()
	defer rs.tables.Unlock()

	// Do nothing if the table is already active.
	if _, ok := rs.tables.active[streamID]; ok {
		return nil
	}

	// If metadata JSON is present then parse it into a usable object. Otherwise
	// initialize new metadata based on discovery results.
	var metadata *mysqlTableMetadata
	if metadataJSON != nil {
		if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
			return fmt.Errorf("error parsing metadata JSON: %w", err)
		}
		// Fix up complex (non-string) column types, since the JSON round-trip
		// will turn *mysqlColumnType values into map[string]interface{}.
		for column, columnType := range metadata.Schema.ColumnTypes {
			if columnType, ok := columnType.(map[string]interface{}); ok {
				var parsedType mysqlColumnType
				if err := mapstructure.Decode(columnType, &parsedType); err == nil {
					metadata.Schema.ColumnTypes[column] = &parsedType
				}
			}
		}
	} else if discovery != nil {
		// If metadata JSON is not present, construct new default metadata based on the discovery info.
		logrus.WithField("stream", streamID).Debug("initializing table metadata")
		metadata = new(mysqlTableMetadata)
		var colTypes = make(map[string]interface{})
		for colName, colInfo := range discovery.Columns {
			colTypes[colName] = colInfo.DataType
		}

		metadata.Schema.Columns = discovery.ColumnNames
		metadata.Schema.ColumnTypes = colTypes

		logrus.WithFields(logrus.Fields{
			"stream":  streamID,
			"columns": metadata.Schema.Columns,
			"types":   metadata.Schema.ColumnTypes,
		}).Debug("initialized table metadata")
	}

	// Finally, mark the table as active and store the updated metadata.
	rs.tables.active[streamID] = struct{}{}
	rs.tables.keyColumns[streamID] = keyColumns
	rs.tables.metadata[streamID] = metadata
	rs.tables.dirtyMetadata = append(rs.tables.dirtyMetadata, streamID)
	return nil
}

func (rs *mysqlReplicationStream) Events() <-chan sqlcapture.DatabaseEvent {
	return rs.events
}

func (rs *mysqlReplicationStream) Acknowledge(ctx context.Context, cursor string) error {
	// No acknowledgements are necessary or possible in MySQL. The binlog is just
	// a series of logfiles on disk which get erased after log rotation according
	// to a time-based retention policy, without any server-side "have all clients
	// consumed these events" tracking.
	//
	// See also: The 'Binlog Retention Sanity Check' logic in source-mysql/main.go
	return nil
}

func (rs *mysqlReplicationStream) Close(ctx context.Context) error {
	rs.cancel()
	return <-rs.errCh
}
