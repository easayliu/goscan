package ddl

import (
	"fmt"
	"strings"
	"testing"

	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
)

func testConfig() *config.Config {
	return &config.Config{ClickHouse: &config.ClickHouseConfig{Database: "logs"}}
}

// Every column must be named once per table. A duplicate would still render a
// valid-looking CREATE, but ClickHouse rejects it and the ALTER would put the
// second copy AFTER itself.
func TestColumnNamesAreUnique(t *testing.T) {
	for _, table := range Tables(testConfig()) {
		seen := make(map[string]bool, len(table.Columns))
		for _, col := range table.Columns {
			if seen[col.Name] {
				t.Errorf("%s: duplicate column %q", table.Name, col.Name)
			}
			seen[col.Name] = true
			if col.Type == "" {
				t.Errorf("%s: column %q has no type", table.Name, col.Name)
			}
		}
	}
}

// The sorting key has to be made of real columns: a typo here only surfaces
// when the DDL Job runs against a live ClickHouse.
func TestOrderByReferencesRealColumns(t *testing.T) {
	for _, table := range Tables(testConfig()) {
		columns := make(map[string]bool, len(table.Columns))
		for _, col := range table.Columns {
			columns[col.Name] = true
		}
		for _, key := range strings.Split(strings.Trim(table.OrderBy, "()"), ",") {
			key = strings.TrimSpace(key)
			if !columns[key] {
				t.Errorf("%s: ORDER BY names %q, which is not a column", table.Name, key)
			}
		}
	}
}

func TestCreateSQLSingleNode(t *testing.T) {
	cfg := testConfig()
	sql := Tables(cfg)[0].CreateSQL(OptionsFrom(cfg))

	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS `logs`.`volcengine_bill_details`") {
		t.Errorf("unexpected CREATE target:\n%s", firstLines(sql, 3))
	}
	for _, unwanted := range []string{"ON CLUSTER", "Distributed", "_local"} {
		if strings.Contains(sql, unwanted) {
			t.Errorf("single node DDL should not mention %q", unwanted)
		}
	}
	if !strings.Contains(sql, "ENGINE = ReplacingMergeTree") {
		t.Error("engine missing from single node DDL")
	}
}

func TestCreateSQLCluster(t *testing.T) {
	cfg := testConfig()
	cfg.ClickHouse.Cluster = "bj_ck"
	cfg.ClickHouse.Replicated = true
	sql := Tables(cfg)[0].CreateSQL(OptionsFrom(cfg))

	want := []string{
		"CREATE TABLE IF NOT EXISTS `logs`.`volcengine_bill_details_local` ON CLUSTER `bj_ck`",
		"ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/logs/volcengine_bill_details_local', '{replica}')",
		"CREATE TABLE IF NOT EXISTS `logs`.`volcengine_bill_details_distributed` ON CLUSTER `bj_ck`",
		"AS `logs`.`volcengine_bill_details_local`",
		"ENGINE = Distributed(`bj_ck`, `logs`, `volcengine_bill_details_local`, rand())",
	}
	for _, fragment := range want {
		if !strings.Contains(sql, fragment) {
			t.Errorf("cluster DDL missing:\n%s", fragment)
		}
	}
}

// The names the DDL creates have to be the names the sync resolves at runtime.
// Getting this wrong is silent until a cluster deployment syncs for the first
// time and every insert reports that the table does not exist, so pin the two
// together rather than to a literal.
func TestTableNamesMatchTheRuntimeResolver(t *testing.T) {
	for _, cluster := range []string{"", "bj_ck"} {
		cfg := testConfig()
		cfg.ClickHouse.Cluster = cluster
		opts := OptionsFrom(cfg)
		resolver := clickhouse.NewTableNameResolver(cfg.ClickHouse)

		for _, table := range Tables(cfg) {
			if got, want := table.TargetTableName(opts), resolver.ResolveInsertTarget(table.Name); got != want {
				t.Errorf("cluster=%q %s: DDL creates %q, the sync writes into %q", cluster, table.Name, got, want)
			}
			if got, want := table.TargetTableName(opts), resolver.ResolveQueryTarget(table.Name); got != want {
				t.Errorf("cluster=%q %s: DDL creates %q, queries read %q", cluster, table.Name, got, want)
			}
			if got, want := table.LocalTableName(opts), resolver.ResolveLocalTableName(table.Name); got != want {
				t.Errorf("cluster=%q %s: DDL local table %q, resolver says %q", cluster, table.Name, got, want)
			}
		}
	}
}

// A cluster without Keeper keeps the plain engine; ReplacingMergeTree() must
// not lose its parentheses on the way.
func TestClusterWithoutReplication(t *testing.T) {
	cfg := testConfig()
	cfg.ClickHouse.Cluster = "bj_ck"
	sql := AliCloudMonthlyTable("alicloud_bill_monthly").CreateSQL(OptionsFrom(cfg))

	if strings.Contains(sql, "Replicated") {
		t.Error("replicated: false still produced a Replicated engine")
	}
	if !strings.Contains(sql, "ENGINE = ReplacingMergeTree()") {
		t.Errorf("local engine changed:\n%s", sql[:200])
	}
	if !strings.Contains(sql, "ENGINE = Distributed(") {
		t.Error("cluster DDL should still create the Distributed table")
	}
}

// Re-running the DDL after an upgrade is how an existing table gets the columns
// a new version added, so every column needs its ADD COLUMN IF NOT EXISTS, in
// the order the CREATE lays them out.
func TestAlterAddsEveryColumnInOrder(t *testing.T) {
	cfg := testConfig()
	table := Tables(cfg)[0]
	sql := table.AlterSQL(OptionsFrom(cfg))

	previous := ""
	for _, col := range table.Columns {
		want := fmt.Sprintf("ADD COLUMN IF NOT EXISTS `%s` %s FIRST", col.Name, columnTypeWithDefault(col))
		if previous != "" {
			want = fmt.Sprintf("ADD COLUMN IF NOT EXISTS `%s` %s AFTER `%s`", col.Name, columnTypeWithDefault(col), previous)
		}
		if !strings.Contains(sql, want) {
			t.Errorf("ALTER missing:\n%s", want)
		}
		previous = col.Name
	}
}

// On a cluster the local table has to be widened before the Distributed table:
// in between, an insert naming the new column would fail.
func TestAlterOrdersLocalBeforeDistributed(t *testing.T) {
	cfg := testConfig()
	cfg.ClickHouse.Cluster = "bj_ck"
	sql := Tables(cfg)[0].AlterSQL(OptionsFrom(cfg))

	local := strings.Index(sql, "ALTER TABLE `logs`.`volcengine_bill_details_local`")
	distributed := strings.Index(sql, "ALTER TABLE `logs`.`volcengine_bill_details_distributed`")
	if local < 0 || distributed < 0 {
		t.Fatalf("expected an ALTER for both tables, got:\n%s", sql)
	}
	if local > distributed {
		t.Error("the Distributed table is altered before its local table")
	}
}

// Renaming a table in the config has to reach the DDL, otherwise the DDL Job
// creates tables the sync never writes into.
func TestTableNamesFollowConfig(t *testing.T) {
	cfg := testConfig()
	cfg.CloudProviders = &config.CloudProvidersConfig{
		VolcEngine: &config.VolcEngineConfig{BillTable: "volc_bills"},
		AliCloud:   &config.AliCloudConfig{MonthlyTable: "ali_monthly", DailyTable: "ali_daily"},
	}

	var names []string
	for _, table := range Tables(cfg) {
		names = append(names, table.Name)
	}
	want := []string{"volc_bills", "ali_monthly", "ali_daily"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("table names = %v, want %v", names, want)
	}
}

// pkg/config cannot import pkg/ddl (that would be a cycle), so the default
// table names exist as literals in both places. Pin them together: if they ever
// drift, the DDL Job creates one table and the sync writes into another.
func TestDefaultsMatchTheConfigDefaults(t *testing.T) {
	volc, ali := config.NewVolcEngineConfig(), config.NewAliCloudConfig()

	if volc.BillTable != DefaultVolcEngineBillTable {
		t.Errorf("config default %q != ddl default %q", volc.BillTable, DefaultVolcEngineBillTable)
	}
	if ali.MonthlyTable != DefaultAliCloudMonthlyTable {
		t.Errorf("config default %q != ddl default %q", ali.MonthlyTable, DefaultAliCloudMonthlyTable)
	}
	if ali.DailyTable != DefaultAliCloudDailyTable {
		t.Errorf("config default %q != ddl default %q", ali.DailyTable, DefaultAliCloudDailyTable)
	}
}

// An empty config must still render something usable rather than a table in a
// database called "".
func TestOptionsFallBackToDefaults(t *testing.T) {
	opts := OptionsFrom(&config.Config{})
	if opts.Database == "" {
		t.Error("database fell back to the empty string")
	}
	if len(Tables(&config.Config{})) != 3 {
		t.Error("expected all three bill tables")
	}
}

// The clause the provider services hand to CreateTable must be complete on its
// own: it is appended straight after the table name.
func TestSchemaClauseIsSelfContained(t *testing.T) {
	clause := AliCloudDailyTable("alicloud_bill_daily").SchemaClause()

	if !strings.HasPrefix(clause, "(") {
		t.Error("clause must open with the column list")
	}
	for _, fragment := range []string{"ENGINE = ReplacingMergeTree()", "PARTITION BY toYYYYMMDD(billing_date)", "ORDER BY (billing_date"} {
		if !strings.Contains(clause, fragment) {
			t.Errorf("clause missing %q", fragment)
		}
	}
	// CreateDistributedTableWithResolver splits the clause on the first
	// ENGINE keyword to reuse the column list.
	if strings.Index(clause, "ENGINE") != strings.LastIndex(clause, "ENGINE") {
		t.Error("ENGINE appears more than once, the column/engine split would cut in the wrong place")
	}
}

func firstLines(s string, n int) string {
	lines := strings.SplitN(s, "\n", n+1)
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}
