package grafana

import (
	"context"
	"net/http"

	"github.com/K-Phoen/grabana"
	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/graph"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/singlestat"
	"github.com/K-Phoen/grabana/table"
	"github.com/K-Phoen/grabana/target/prometheus"
	"github.com/K-Phoen/grabana/variable/interval"
)

var (
	folderName = "Matrixcube"
)

// DashboardCreator matrixcube grafana dashboard creator
type DashboardCreator struct {
	cli        *grabana.Client
	dataSource string
}

// NewDashboardCreator returns a dashboard creator
// eyJrIjoiQU5nZWJNOWNha0JJRDdQTTFWSnVSanFBMkhscGMyaksiLCJuIjoiYXBpa2V5MDAxIiwiaWQiOjF9
func NewDashboardCreator(grafana, apiKey, dataSource string) *DashboardCreator {
	return &DashboardCreator{
		cli:        grabana.NewClient(http.DefaultClient, grafana, apiKey),
		dataSource: dataSource,
	}
}

// Create create dashboard
func (c *DashboardCreator) Create() error {
	folder, err := c.createFolder()
	if err != nil {
		return err
	}

	return c.createRaftDashboard(folder)
}

func (c *DashboardCreator) createFolder() (*grabana.Folder, error) {
	folder, err := c.cli.GetFolderByTitle(context.Background(), folderName)
	if err != nil && err != grabana.ErrFolderNotFound {
		return nil, err
	}

	if folder == nil {
		folder, err = c.cli.CreateFolder(context.Background(), folderName)
		if err != nil {
			return nil, err
		}
	}

	return folder, nil
}

func (c *DashboardCreator) createRaftDashboard(folder *grabana.Folder) error {
	db := grabana.NewDashboardBuilder("Raftstore Status",
		grabana.AutoRefresh("5s"),
		grabana.Tags([]string{"generated"}),
		grabana.VariableAsInterval(
			"interval",
			interval.Values([]string{"30s", "1m", "5m", "10m", "30m", "1h", "6h", "12h"}),
		),
		c.storageRow(),
		c.requestRow(),
		c.shardsRow(),
		c.raftLogRow(),
		c.raftInternalRow(),
		c.promgramInternalRow())

	_, err := c.cli.UpsertDashboard(context.Background(), folder, db)
	return err
}

func (c *DashboardCreator) raftLogRow() grabana.DashboardBuilderOption {
	return grabana.Row(
		"Raft log status",
		c.withGraph("50% raft log append time", 4,
			`histogram_quantile(0.50, sum(rate(matrixcube_raftstore_raft_log_append_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("s"), axis.Min(0)),
		c.withGraph("99% raft log append time", 4,
			`histogram_quantile(0.99, sum(rate(matrixcube_raftstore_raft_log_append_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("s"), axis.Min(0)),
		c.withGraph("99.99% raft log append time", 4,
			`histogram_quantile(0.9999, sum(rate(matrixcube_raftstore_raft_log_append_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("s"), axis.Min(0)),

		c.withGraph("50% raft log applied time", 4,
			`histogram_quantile(0.50, sum(rate(matrixcube_raftstore_raft_log_apply_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("s"), axis.Min(0)),
		c.withGraph("99% raft log applied time", 4,
			`histogram_quantile(0.99, sum(rate(matrixcube_raftstore_raft_log_apply_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("s"), axis.Min(0)),
		c.withGraph("99.99% raft log applied time", 4,
			`histogram_quantile(0.9999, sum(rate(matrixcube_raftstore_raft_log_apply_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("s"), axis.Min(0)),

		c.withGraph("50% raft log size", 4,
			`histogram_quantile(0.50, sum(rate(matrixcube_raftstore_raft_proposal_log_bytes_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("bytes"), axis.Min(0)),
		c.withGraph("99% raft log size", 4,
			`histogram_quantile(0.99, sum(rate(matrixcube_raftstore_raft_proposal_log_bytes_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("bytes"), axis.Min(0)),
		c.withGraph("99.99% raft log size", 4,
			`histogram_quantile(0.9999, sum(rate(matrixcube_raftstore_raft_proposal_log_bytes_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("bytes"), axis.Min(0)),

		c.withGraph("50% raft snapshot size", 4,
			`histogram_quantile(0.50, sum(rate(matrixcube_raftstore_snapshot_size_bytes_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("bytes"), axis.Min(0)),
		c.withGraph("99% raft snapshot size", 4,
			`histogram_quantile(0.99, sum(rate(matrixcube_raftstore_snapshot_size_bytes_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("bytes"), axis.Min(0)),
		c.withGraph("99.99% raft snapshot size", 4,
			`histogram_quantile(0.9999, sum(rate(matrixcube_raftstore_snapshot_size_bytes_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("bytes"), axis.Min(0)),

		c.withGraph("50% raft snapshot build time", 4,
			`histogram_quantile(0.50, sum(rate(matrixcube_raftstore_snapshot_building_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("s"), axis.Min(0)),
		c.withGraph("99% raft snapshot build time", 4,
			`histogram_quantile(0.99, sum(rate(matrixcube_raftstore_snapshot_building_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("s"), axis.Min(0)),
		c.withGraph("99.99% raft snapshot build time", 4,
			`histogram_quantile(0.9999, sum(rate(matrixcube_raftstore_snapshot_building_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}", axis.Unit("s"), axis.Min(0)),
	)
}

func (c *DashboardCreator) raftInternalRow() grabana.DashboardBuilderOption {
	return grabana.Row(
		"Raft internal status",
		c.withGraph("Raft normal commands", 3,
			"sum(rate(matrixcube_raftstore_command_normal_total[$interval])) by (type)",
			"{{ type }}"),
		c.withGraph("Raft admin commands", 3,
			"sum(rate(matrixcube_raftstore_command_admin_total[$interval])) by (type, status)",
			"{{ type }}({{ status }})"),
		c.withGraph("Handled raft event", 3,
			"sum(rate(matrixcube_raftstore_raft_ready_handled_total[$interval])) by (type)",
			"{{ type }}"),
		c.withGraph("Sended raft messages", 3,
			"sum(rate(matrixcube_raftstore_raft_sent_msg_total[$interval])) by (type)",
			"{{ type }}"),
	)
}

func (c *DashboardCreator) shardsRow() grabana.DashboardBuilderOption {
	return grabana.Row(
		"Shards status",
		c.withTable("Shards count overview", 4,
			"sum(matrixcube_raftstore_store_shard_total) by (type)",
			"{{ type }}"),
		c.withTable("Shards count per node", 4,
			`sum(matrixcube_raftstore_store_shard_total{type="shards"}) by (instance)`,
			"{{ instance }}"),
		c.withTable("Leader count per node", 4,
			`sum(matrixcube_raftstore_store_shard_total{type="leader"}) by (instance)`,
			"{{ instance }}"),
	)
}

func (c *DashboardCreator) promgramInternalRow() grabana.DashboardBuilderOption {
	return grabana.Row(
		"Promgram internal status",
		c.withGraph("Queue", 6,
			"sum(matrixcube_raftstore_queue_size) by (type)",
			"{{ type }}"),
		c.withGraph("Batching", 6,
			"sum(matrixcube_raftstore_batch_size) by (type)",
			"{{ type }}"),
	)
}

func (c *DashboardCreator) requestRow() grabana.DashboardBuilderOption {
	return grabana.Row(
		"Request status",
		c.withGraph("Requests received", 12,
			"sum(rate(matrixcube_raftstore_command_normal_total[$interval]))",
			"All requests"),
	)
}

func (c *DashboardCreator) storageRow() grabana.DashboardBuilderOption {
	return grabana.Row(
		"Overview status",
		row.WithSingleStat(
			"Storage Total",
			singlestat.Height("200px"),
			singlestat.Span(4),
			singlestat.WithPrometheusTarget(
				"sum(matrixcube_raftstore_store_storage_bytes{type='total'})"),
			singlestat.Unit("bytes"),
		),
		row.WithSingleStat(
			"Storage Used",
			singlestat.Height("200px"),
			singlestat.Span(4),
			singlestat.WithPrometheusTarget(
				"sum(matrixcube_raftstore_store_storage_bytes{type='total'})-sum(matrixcube_raftstore_store_storage_bytes{type='free'})"),
			singlestat.Unit("bytes"),
		),
		row.WithSingleStat(
			"Storage Available",
			singlestat.Height("200px"),
			singlestat.Span(4),
			singlestat.WithPrometheusTarget(
				"sum(matrixcube_raftstore_store_storage_bytes{type='free'})"),
			singlestat.Unit("bytes"),
		),
	)
}

func (c *DashboardCreator) withGraph(title string, span float32, pql string, legend string, opts ...axis.Option) row.Option {
	return row.WithGraph(
		title,
		graph.Span(span),
		graph.Height("400px"),
		graph.DataSource(c.dataSource),
		graph.WithPrometheusTarget(
			pql,
			prometheus.Legend(legend),
		),
		graph.LeftYAxis(opts...),
	)
}

func (c *DashboardCreator) withTable(title string, span float32, pql string, legend string) row.Option {
	return row.WithTable(
		title,
		table.Span(span),
		table.Height("400px"),
		table.DataSource(c.dataSource),
		table.WithPrometheusTarget(
			pql,
			prometheus.Legend(legend)),
		table.AsTimeSeriesAggregations([]table.Aggregation{
			{Label: "Current", Type: table.Current},
			{Label: "Max", Type: table.Max},
			{Label: "Min", Type: table.Min},
		}),
	)
}
