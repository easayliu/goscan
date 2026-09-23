// Package ddl holds the canonical ClickHouse schema of every bill table goscan
// writes, plus the rendering of those definitions into SQL.
//
// The definitions live here and nowhere else: `goscan --ddl` prints them for the
// DDL Job to apply, and the provider services build their CREATE TABLE clause
// from the same structs. A column added in one place therefore cannot drift from
// the table the sync actually writes into.
package ddl

import (
	"fmt"
	"strings"
)

// Column is one ClickHouse column of a bill table.
type Column struct {
	Name    string
	Type    string
	Default string // DEFAULT expression, empty when the column has none
	Section string // group heading emitted above this column, for readability
	Comment string // trailing comment
}

// definition renders the column as it appears inside CREATE TABLE.
func (c Column) definition() string {
	def := fmt.Sprintf("%s %s", c.Name, c.Type)
	if c.Default != "" {
		def += " DEFAULT " + c.Default
	}
	return def
}

// Table is the canonical definition of one bill table.
type Table struct {
	// Name is the table the sync reads and writes. In cluster mode it is the
	// Distributed table and the local tables under it get a _local suffix.
	Name        string
	Comment     string // what the table holds, emitted above CREATE TABLE
	Columns     []Column
	Engine      string // engine of the table that stores the data
	PartitionBy string
	OrderBy     string
	// ShardBy is the Distributed table's sharding key. It must be a function of
	// the sorting key and nothing else, because the sorting key is what
	// ReplacingMergeTree deduplicates on and that deduplication only ever
	// happens inside one shard: with rand() the two copies of a re-pulled row
	// land on different shards, neither a background merge nor FINAL can see
	// both, and the amounts add up twice. Empty falls back to rand(), which is
	// only safe for a table nothing ever writes twice.
	ShardBy  string
	Settings string
}

// Options describes the ClickHouse the DDL is rendered for.
type Options struct {
	Database string
	// Cluster is the ClickHouse cluster name (the one in system.clusters, not
	// the k8s cluster). Empty means a single node: one plain table, no
	// ON CLUSTER, no Distributed table.
	Cluster string
	// Replicated turns the local table into its Replicated* engine. It needs
	// ClickHouse Keeper / ZooKeeper; clusters made of unreplicated shards must
	// set it to false.
	Replicated bool
}

// LocalTableName is the table that actually stores the rows: the plain name on
// a single node, the per-node table under the Distributed one on a cluster.
//
// The two names below must agree with clickhouse.TableNameResolver, which is
// what the sync resolves its INSERT and SELECT targets through at runtime —
// tables created under any other name are tables nothing ever writes to.
// ddl_test.go checks both against the resolver.
func (t Table) LocalTableName(o Options) string {
	if o.Cluster == "" {
		return t.Name
	}
	return t.Name + "_local"
}

// TargetTableName is the table the sync writes into and queries read from. It
// is the plain name in both modes: on a single node that is the table itself,
// on a cluster it is the Distributed table sitting on top of the _local ones.
// This is the same naming logpipe / tracepipe / metricpipe use, so a reader
// such as opdash can point at one name and have it work in either deployment.
func (t Table) TargetTableName(o Options) string {
	return t.Name
}

// SchemaClause renders everything that follows the table name in a CREATE TABLE
// statement: the column list, the engine and the layout. This is the form the
// ClickHouse client's CreateTable helpers take.
func (t Table) SchemaClause() string {
	var b strings.Builder
	b.WriteString("(\n")
	for i, col := range t.Columns {
		if col.Section != "" {
			if i > 0 {
				b.WriteString("\n")
			}
			fmt.Fprintf(&b, "\t-- %s\n", col.Section)
		}
		fmt.Fprintf(&b, "\t%s", col.definition())
		if i < len(t.Columns)-1 {
			b.WriteString(",")
		}
		if col.Comment != "" {
			fmt.Fprintf(&b, " -- %s", col.Comment)
		}
		b.WriteString("\n")
	}
	b.WriteString(")\n")
	fmt.Fprintf(&b, "ENGINE = %s\n", t.Engine)
	fmt.Fprintf(&b, "PARTITION BY %s\n", t.PartitionBy)
	fmt.Fprintf(&b, "ORDER BY %s", t.OrderBy)
	if t.Settings != "" {
		fmt.Fprintf(&b, "\nSETTINGS %s", t.Settings)
	}
	return b.String()
}

// engineFor returns the engine of the data-holding table. On a replicated
// cluster the plain MergeTree family engine becomes its Replicated* twin;
// {shard} and {replica} are ClickHouse's own macros, expanded per node from
// each server's <macros> config, not something to substitute here.
func (t Table) engineFor(o Options) string {
	if o.Cluster == "" || !o.Replicated {
		return t.Engine
	}
	local := t.LocalTableName(o)
	name, args, _ := strings.Cut(t.Engine, "(")
	name = strings.TrimSpace(name)
	path := fmt.Sprintf("'/clickhouse/tables/{shard}/%s/%s', '{replica}'", o.Database, local)
	// ReplacingMergeTree() and ReplacingMergeTree(version) both keep their
	// arguments, they just move behind the keeper path.
	if rest := strings.TrimSpace(strings.TrimSuffix(args, ")")); rest != "" {
		path += ", " + rest
	}
	return fmt.Sprintf("Replicated%s(%s)", name, path)
}

// CreateSQL renders the CREATE TABLE statements: one on a single node, two on a
// cluster (the local table that stores the rows, then the Distributed table the
// sync writes into and queries read from).
func (t Table) CreateSQL(o Options) string {
	local := t.LocalTableName(o)
	body := t.SchemaClause()
	// The engine and the layout come from SchemaClause; on a cluster only the
	// engine line differs, so swap that one line out.
	body = strings.Replace(body,
		fmt.Sprintf("ENGINE = %s\n", t.Engine),
		fmt.Sprintf("ENGINE = %s\n", t.engineFor(o)), 1)

	if o.Cluster == "" {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`\n%s;", o.Database, t.Name, body)
	}

	target := t.TargetTableName(o)
	onCluster := fmt.Sprintf(" ON CLUSTER `%s`", o.Cluster)
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s`%s\n%s;\n\n"+
			"CREATE TABLE IF NOT EXISTS `%s`.`%s`%s\nAS `%s`.`%s`\nENGINE = Distributed(`%s`, `%s`, `%s`, %s);",
		o.Database, local, onCluster, body,
		o.Database, target, onCluster, o.Database, local,
		o.Cluster, o.Database, local, t.shardingKey())
}

// shardingKey is what the Distributed table routes on. See Table.ShardBy for
// why it may not be rand().
func (t Table) shardingKey() string {
	if t.ShardBy == "" {
		return "rand()"
	}
	return t.ShardBy
}

// AlterSQL renders the statements that bring an existing table up to the
// definition above: every column is added IF NOT EXISTS, in place, so running
// the DDL again after an upgrade backfills what a table is missing and does
// nothing at all when it is already current.
func (t Table) AlterSQL(o Options) string {
	alter := func(target, onCluster string) string {
		actions := make([]string, 0, len(t.Columns))
		previous := ""
		for _, col := range t.Columns {
			// Keep new columns in their defined position rather than letting
			// them pile up at the end. AFTER only rewrites metadata, and the
			// column it names either already exists or was added just above.
			where := "FIRST"
			if previous != "" {
				where = fmt.Sprintf("AFTER `%s`", previous)
			}
			actions = append(actions, fmt.Sprintf("ADD COLUMN IF NOT EXISTS `%s` %s %s",
				col.Name, columnTypeWithDefault(col), where))
			previous = col.Name
		}
		return fmt.Sprintf("ALTER TABLE `%s`.`%s`%s\n    %s;",
			o.Database, target, onCluster, strings.Join(actions, ",\n    "))
	}

	if o.Cluster == "" {
		return alter(t.Name, "")
	}

	// The local table first: the other way round there is a moment where the
	// Distributed table has a column its local tables do not, and an insert
	// naming it fails.
	onCluster := fmt.Sprintf(" ON CLUSTER `%s`", o.Cluster)
	return alter(t.LocalTableName(o), onCluster) + "\n\n" + alter(t.TargetTableName(o), onCluster)
}

func columnTypeWithDefault(c Column) string {
	if c.Default != "" {
		return fmt.Sprintf("%s DEFAULT %s", c.Type, c.Default)
	}
	return c.Type
}

// Render returns the complete DDL script for the given tables: the CREATE
// statements followed by the idempotent ALTERs. It never touches ClickHouse —
// the output is meant to be piped into clickhouse-client.
//
// What it cannot do is change a table that already exists in a different shape:
// CREATE TABLE IF NOT EXISTS is a no-op there and the ALTERs only add columns.
// A table created before the schema change of 2026-09 (amounts as String, the
// payable amount inside the sorting key, rand() sharding) therefore has to be
// dropped and recreated — the header below says so, and the bill tables are
// cheap to refill: point the sync at the periods again. The same goes for the
// Alibaba Cloud tables created before item and line_seq joined their sorting
// key and their amounts became Decimal.
//
// CREATE DATABASE is deliberately not part of it: creating the database needs
// ON CLUSTER too, and whether to create it at all is the operator's call.
func Render(tables []Table, o Options) string {
	var b strings.Builder
	b.WriteString("-- Generated by `goscan --ddl`. Safe to apply repeatedly:\n")
	b.WriteString("-- every statement is IF NOT EXISTS, so re-running it after an upgrade\n")
	b.WriteString("-- only backfills the columns an existing table is missing.\n")
	b.WriteString("--\n")
	b.WriteString("-- It cannot reshape a table that already exists: engine, sorting key,\n")
	b.WriteString("-- partition key and column types are fixed at CREATE. Tables created before\n")
	b.WriteString("-- 2026-09 (String amounts, PayableAmount / payment_amount in the sorting key,\n")
	b.WriteString("-- rand() sharding) must be dropped and recreated, then re-synced.\n")
	b.WriteString("-- The Alibaba Cloud tables changed again later in 2026-09 (Float64 amounts,\n")
	b.WriteString("-- no item / line_seq in the sorting key): ones created before that must be\n")
	b.WriteString("-- dropped and recreated too. The backfill below would add the two columns\n")
	b.WriteString("-- but not put them in the key, and lines sharing a key would still merge away.\n")
	b.WriteString("-- The VolcEngine table only changed RoundAmount to Decimal, which\n")
	b.WriteString("-- ALTER TABLE ... MODIFY COLUMN can do in place.\n")
	if o.Cluster != "" {
		fmt.Fprintf(&b, "-- Cluster mode (`%s`): rows live in the _local tables and the sync writes\n", o.Cluster)
		b.WriteString("-- into the Distributed tables of the same name on top of them.\n")
	}
	fmt.Fprintf(&b, "-- Database `%s` must exist already.\n", o.Database)

	for _, t := range tables {
		b.WriteString("\n")
		if t.Comment != "" {
			fmt.Fprintf(&b, "\n-- %s\n", t.Comment)
		}
		b.WriteString(t.CreateSQL(o))
		b.WriteString("\n\n")
		b.WriteString(t.AlterSQL(o))
		b.WriteString("\n")
	}
	return b.String()
}
