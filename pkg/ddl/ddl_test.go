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

	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS `logs`.`volcengine_bill`") {
		t.Errorf("unexpected CREATE target:\n%s", firstLines(sql, 3))
	}
	for _, unwanted := range []string{"ON CLUSTER", "Distributed", "_local"} {
		if strings.Contains(sql, unwanted) {
			t.Errorf("single node DDL should not mention %q", unwanted)
		}
	}
	if !strings.Contains(sql, "ENGINE = ReplacingMergeTree(updated_at)") {
		t.Error("engine missing from single node DDL")
	}
}

func TestCreateSQLCluster(t *testing.T) {
	cfg := testConfig()
	cfg.ClickHouse.Cluster = "bj_ck"
	cfg.ClickHouse.Replicated = true
	sql := Tables(cfg)[0].CreateSQL(OptionsFrom(cfg))

	want := []string{
		"CREATE TABLE IF NOT EXISTS `logs`.`volcengine_bill_local` ON CLUSTER `bj_ck`",
		"ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/logs/volcengine_bill_local', '{replica}', updated_at)",
		"CREATE TABLE IF NOT EXISTS `logs`.`volcengine_bill` ON CLUSTER `bj_ck`",
		"AS `logs`.`volcengine_bill_local`",
		"ENGINE = Distributed(`bj_ck`, `logs`, `volcengine_bill_local`, cityHash64(BillPeriod, ExpenseDate, InstanceNo, ExpenseBeginTime, Product, ElementCode, BillDetailId))",
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
	if !strings.Contains(sql, "ENGINE = ReplacingMergeTree(updated_at)") {
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

	local := strings.Index(sql, "ALTER TABLE `logs`.`volcengine_bill_local`")
	distributed := strings.Index(sql, "ALTER TABLE `logs`.`volcengine_bill` ON")
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
	for _, fragment := range []string{"ENGINE = ReplacingMergeTree(updated_at)", "PARTITION BY toYYYYMMDD(billing_date)", "ORDER BY (billing_date"} {
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

// orderByColumns lists the sorting key的列名, for the tests below.
func orderByColumns(table Table) []string {
	var out []string
	for _, key := range strings.Split(strings.Trim(table.OrderBy, "()"), ",") {
		out = append(out, strings.TrimSpace(key))
	}
	return out
}

// An amount must never take part in the sorting key. The cloud corrects bills
// after the fact (refunds, recomputed discounts, invoice adjustments); with the
// amount in the key, the corrected row gets a different key and settles NEXT TO
// the old one instead of replacing it, and the period is billed twice.
func TestSortingKeyCarriesNoAmount(t *testing.T) {
	for _, table := range Tables(testConfig()) {
		types := make(map[string]string, len(table.Columns))
		for _, col := range table.Columns {
			types[col.Name] = col.Type
		}
		for _, key := range orderByColumns(table) {
			switch types[key] {
			case MoneyType, "Float64":
				t.Errorf("%s: ORDER BY includes the amount column %q", table.Name, key)
			}
		}
	}
}

// ReplacingMergeTree keeps the row with the highest version. Without one it
// keeps an arbitrary row, so a re-pull of a corrected period could resurrect the
// stale amount.
func TestEngineVersionsRowsByUpdatedAt(t *testing.T) {
	for _, table := range Tables(testConfig()) {
		if !strings.Contains(table.Engine, "ReplacingMergeTree(updated_at)") {
			t.Errorf("%s: engine is %q, want ReplacingMergeTree(updated_at)", table.Name, table.Engine)
		}
		found := false
		for _, col := range table.Columns {
			if col.Name == "updated_at" {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: engine versions on updated_at, which is not a column", table.Name)
		}
	}
}

// The sharding key has to be a function of the sorting key and nothing else.
// Any other expression (rand() above all) can send the two copies of a re-pulled
// row to different shards, where no merge and no FINAL will ever see them
// together — the deduplication the engine promises silently stops applying.
func TestShardingKeyIsDerivedFromTheSortingKey(t *testing.T) {
	for _, table := range Tables(testConfig()) {
		want := "cityHash64(" + strings.Trim(table.OrderBy, "()") + ")"
		if table.ShardBy != want {
			t.Errorf("%s: sharding key %q, want %q", table.Name, table.ShardBy, want)
		}
	}
}

// Partition expressions run on String columns the provider fills in. The
// throwing parsers take the whole INSERT down when one row comes back with an
// empty date, which is a bad way to learn that a provider left a field out.
func TestPartitionKeysParseLeniently(t *testing.T) {
	for _, table := range Tables(testConfig()) {
		for _, forbidden := range []string{"toDate(ExpenseDate)", "parseDateTimeBestEffort("} {
			if strings.Contains(table.PartitionBy, forbidden) {
				t.Errorf("%s: PARTITION BY %q still uses the throwing %q", table.Name, table.PartitionBy, forbidden)
			}
		}
	}
}

// Amounts are Decimal, not String and not Float64: see MoneyType.
func TestAmountColumnsAreDecimal(t *testing.T) {
	amounts := []string{"PayableAmount", "PaidAmount", "OriginalBillAmount", "PretaxAmount", "CouponAmount", "Price"}
	types := make(map[string]string)
	for _, col := range VolcEngineBillTable("volcengine_bill").Columns {
		types[col.Name] = col.Type
	}
	for _, name := range amounts {
		if types[name] != MoneyType {
			t.Errorf("%s is %q, want %s", name, types[name], MoneyType)
		}
	}
}

func firstLines(s string, n int) string {
	lines := strings.SplitN(s, "\n", n+1)
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}

// --drop-legacy must never name a table the new DDL creates: applied in the
// documented order it would then drop the table that was just built.
func TestDropLegacyLeavesCurrentTablesAlone(t *testing.T) {
	for _, cluster := range []string{"", "bj_ck"} {
		cfg := testConfig()
		cfg.ClickHouse.Cluster = cluster
		o := OptionsFrom(cfg)
		sql := DropLegacySQL(cfg, o)

		for _, table := range Tables(cfg) {
			for _, current := range []string{table.TargetTableName(o), table.LocalTableName(o)} {
				// The closing backtick is what keeps `volcengine_bill` from
				// matching the `volcengine_bill_details` line above it.
				dropped := fmt.Sprintf("DROP TABLE IF EXISTS `logs`.`%s`", current)
				if strings.Contains(sql, dropped) {
					t.Errorf("cluster=%q: drops the table in use %q:\n%s", cluster, current, sql)
				}
			}
		}
	}
}

// The tables the old naming created have to actually be listed, otherwise an
// upgraded cluster keeps three Distributed shells nothing writes to.
func TestDropLegacyCoversTheOldNames(t *testing.T) {
	cfg := testConfig()
	cfg.ClickHouse.Cluster = "bj_ck"
	sql := DropLegacySQL(cfg, OptionsFrom(cfg))

	want := []string{
		"DROP TABLE IF EXISTS `logs`.`volcengine_bill_details_distributed` ON CLUSTER `bj_ck`;",
		"DROP TABLE IF EXISTS `logs`.`volcengine_bill_details_local` ON CLUSTER `bj_ck`;",
		"DROP TABLE IF EXISTS `logs`.`alicloud_bill_monthly_distributed` ON CLUSTER `bj_ck`;",
		"DROP TABLE IF EXISTS `logs`.`alicloud_bill_daily_distributed` ON CLUSTER `bj_ck`;",
	}
	for _, fragment := range want {
		if !strings.Contains(sql, fragment) {
			t.Errorf("drop-legacy missing:\n%s\ngot:\n%s", fragment, sql)
		}
	}
}

// A config that still pins the old VolcEngine name is using that table, not
// carrying it over from an old release — dropping it would delete live data.
func TestDropLegacyKeepsAPinnedLegacyName(t *testing.T) {
	cfg := testConfig()
	cfg.CloudProviders = &config.CloudProvidersConfig{
		VolcEngine: &config.VolcEngineConfig{BillTable: LegacyVolcEngineBillTable},
	}
	sql := DropLegacySQL(cfg, OptionsFrom(cfg))

	if strings.Contains(sql, "DROP TABLE") {
		t.Errorf("single node with the legacy name pinned has nothing to drop, got:\n%s", sql)
	}
}
