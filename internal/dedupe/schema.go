package dedupe

import (
	"fmt"

	"github.com/gocql/gocql"
)

// EnsureSchema creates the ScyllaDB keyspace and dedupe table if they do not
// already exist. It connects without a keyspace (bootstrap session), runs the
// DDL, and closes the session. The keyspace uses SimpleStrategy with RF=1 as a
// dev-friendly default; production operators should pre-create their keyspace
// with the desired replication strategy — IF NOT EXISTS will leave it untouched.
func EnsureSchema(hosts []string, keyspace string) error {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("ensure scylla schema: connect: %w", err)
	}
	defer session.Close()

	createKeyspace := fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
		keyspace)
	if err := session.Query(createKeyspace).Exec(); err != nil {
		return fmt.Errorf("ensure scylla schema: create keyspace: %w", err)
	}

	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.dedupe (
			event_hash text,
			created_at timestamp,
			PRIMARY KEY (event_hash)
		)`, keyspace)
	if err := session.Query(createTable).Exec(); err != nil {
		return fmt.Errorf("ensure scylla schema: create table: %w", err)
	}

	return nil
}
