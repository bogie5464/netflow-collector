package sinktest

import (
	"context"
	"fmt"
	"testing"
	"time"

	// Registers the "clickhouse" database/sql driver that wait.ForSQL probes with.
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ClickHouseImage is copied literally from docker-compose.yml, which owns it.
const ClickHouseImage = "clickhouse/clickhouse-server:25.8.33.6"

// StartClickHouse starts a throwaway ClickHouse container and returns its
// native-protocol DSN. It waits with an explicit SQL probe rather than
// scraping logs.
func StartClickHouse(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dsnFor := func(host string, port network.Port) string {
		return fmt.Sprintf("clickhouse://netflow:netflow@%s:%s/netflow", host, port.Port())
	}
	c, err := testcontainers.Run(ctx, ClickHouseImage,
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_DB":       "netflow",
			"CLICKHOUSE_USER":     "netflow",
			"CLICKHOUSE_PASSWORD": "netflow",
		}),
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForSQL("9000/tcp", "clickhouse", dsnFor).WithStartupTimeout(2*time.Minute)),
	)
	require.NoError(t, err, "start %s", ClickHouseImage)
	t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(c)) })
	host, err := c.Host(ctx)
	require.NoError(t, err)
	port, err := c.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)
	return dsnFor(host, port)
}
