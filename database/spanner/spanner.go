package spanner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	nurl "net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"cloud.google.com/go/spanner"
	sdb "cloud.google.com/go/spanner/admin/database/apiv1"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/token"
	"github.com/zredinger-ccc/migrate/v4"
	"github.com/zredinger-ccc/migrate/v4/database"

	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
)

func init() {
	db := Spanner{}
	database.Register("spanner", &db)
}

// DefaultMigrationsTable is used if no custom table is specified
const DefaultMigrationsTable = "SchemaMigrations"

// Driver errors
var (
	ErrNilConfig      = errors.New("no config")
	ErrNoDatabaseName = errors.New("no database name")
	ErrNoSchema       = errors.New("no schema")
	ErrDatabaseDirty  = errors.New("database is dirty")
	ErrLockHeld       = errors.New("unable to obtain lock")
	ErrLockNotHeld    = errors.New("unable to release already released lock")
)

// Config used for a Spanner instance
type Config struct {
	MigrationsTable string
	DatabaseName    string
	// Whether to parse the migration DDL with spansql before
	// running them towards Spanner.
	// Parsing outputs clean DDL statements such as reformatted
	// and void of comments.
	CleanStatements bool
}

// Spanner implements database.Driver for Google Cloud Spanner
type Spanner struct {
	db     *DB
	config *Config
	lock   atomic.Bool
}

type DB struct {
	admin  *sdb.DatabaseAdminClient
	data   *spanner.Client
	shared bool
}

func NewDB(admin sdb.DatabaseAdminClient, data spanner.Client) *DB {
	return &DB{
		admin:  &admin,
		data:   &data,
		shared: true,
	}
}

// WithInstance implements database.Driver
func WithInstance(instance *DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if len(config.DatabaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	sx := &Spanner{
		db:     instance,
		config: config,
	}

	if err := sx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return sx, nil
}

// Open implements database.Driver
func (s *Spanner) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	adminClient, err := sdb.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	dbname := strings.Replace(migrate.FilterCustomQuery(purl).String(), "spanner://", "", 1)
	dataClient, err := spanner.NewClient(ctx, dbname)
	if err != nil {
		log.Fatal(err)
	}

	migrationsTable := purl.Query().Get("x-migrations-table")

	cleanQuery := purl.Query().Get("x-clean-statements")
	clean := false
	if cleanQuery != "" {
		clean, err = strconv.ParseBool(cleanQuery)
		if err != nil {
			return nil, err
		}
	}

	db := &DB{admin: adminClient, data: dataClient}
	return WithInstance(db, &Config{
		DatabaseName:    dbname,
		MigrationsTable: migrationsTable,
		CleanStatements: clean,
	})
}

// Close implements database.Driver
func (s *Spanner) Close() error {
	if s.db.shared {
		return nil
	}
	s.db.data.Close()
	return s.db.admin.Close()
}

// Lock implements database.Driver but doesn't do anything because Spanner only
// enqueues the UpdateDatabaseDdlRequest.
func (s *Spanner) Lock() error {
	if swapped := s.lock.CompareAndSwap(false, true); swapped {
		return nil
	}
	return ErrLockHeld
}

// Unlock implements database.Driver but no action required, see Lock.
func (s *Spanner) Unlock() error {
	if swapped := s.lock.CompareAndSwap(true, false); swapped {
		return nil
	}
	return ErrLockNotHeld
}

// Run implements database.Driver
func (s *Spanner) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx := context.Background()

	if !s.config.CleanStatements {
		return s.runDdl(ctx, []string{string(migr)})
	}

	stmtGroups, err := statementGroups(migr)
	if err != nil {
		return err
	}

	for _, group := range stmtGroups {
		switch group.typ {
		case statementTypeDDL:
			if err := s.runDdl(ctx, group.stmts); err != nil {
				return err
			}
		case statementTypeDML:
			if err := s.runDml(ctx, group.stmts); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown statement type: %s", group.typ)
		}
	}

	return nil
}

func (s *Spanner) runDdl(ctx context.Context, stmts []string) error {
	op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: stmts,
	})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: []byte(strings.Join(stmts, ";\n"))}
	}

	if err := op.Wait(ctx); err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: []byte(strings.Join(stmts, ";\n"))}
	}

	return nil
}

func (s *Spanner) runDml(ctx context.Context, stmts []string) error {
	_, err := s.db.data.ReadWriteTransaction(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			for _, s := range stmts {
				_, err := txn.Update(ctx, spanner.Statement{SQL: s})
				if err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: []byte(strings.Join(stmts, ";\n"))}
	}

	return nil
}

// SetVersion implements database.Driver
func (s *Spanner) SetVersion(version int, dirty bool) error {
	ctx := context.Background()

	_, err := s.db.data.ReadWriteTransaction(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			m := []*spanner.Mutation{
				spanner.Delete(s.config.MigrationsTable, spanner.AllKeys()),
				spanner.Insert(s.config.MigrationsTable,
					[]string{"Version", "Dirty"},
					[]interface{}{version, dirty},
				),
			}
			return txn.BufferWrite(m)
		})
	if err != nil {
		return &database.Error{OrigErr: err}
	}

	return nil
}

// Version implements database.Driver
func (s *Spanner) Version() (version int, dirty bool, err error) {
	ctx := context.Background()

	stmt := spanner.Statement{
		SQL: `SELECT Version, Dirty FROM ` + s.config.MigrationsTable + ` LIMIT 1`,
	}
	iter := s.db.data.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	switch err {
	case iterator.Done:
		return database.NilVersion, false, nil
	case nil:
		var v int64
		if err = row.Columns(&v, &dirty); err != nil {
			return 0, false, &database.Error{OrigErr: err, Query: []byte(stmt.SQL)}
		}
		version = int(v)
	default:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(stmt.SQL)}
	}

	return version, dirty, nil
}

// Drop implements database.Driver. Retrieves the database schema first and
// creates statements to drop the indexes and tables accordingly.
// Note: The drop statements are created in reverse order to how they're
// provided in the schema. Assuming the schema describes how the database can
// be "build up", it seems logical to "unbuild" the database simply by going the
// opposite direction. More testing
func (s *Spanner) Drop() error {
	ctx := context.Background()
	res, err := s.db.admin.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{
		Database: s.config.DatabaseName,
	})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "drop failed"}
	}
	if len(res.Statements) == 0 {
		return nil
	}
	
	viewDropStatements, err := s.viewDropStatements(ctx)
	if err != nil {
		return err
	}
	if len(viewDropStatements) > 0{
		op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   s.config.DatabaseName,
			Statements: viewDropStatements,
		})
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(strings.Join(viewDropStatements, "; "))}
		}
		if err := op.Wait(ctx); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(strings.Join(viewDropStatements, "; "))}
		}
	}
	
	constraintDropStatements, err := s.constraintDropStatements(ctx)
	if err != nil {
		return err
	}
	if len(constraintDropStatements) > 0 {
		op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   s.config.DatabaseName,
			Statements: constraintDropStatements,
		})
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(strings.Join(constraintDropStatements, "; "))}
		}
		if err := op.Wait(ctx); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(strings.Join(constraintDropStatements, "; "))}
		}
	}

	tableDropStatements, err := s.tableDropStatements(ctx)
	if err != nil {
		return err
	}
	if len(tableDropStatements) > 0 {
		op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   s.config.DatabaseName,
			Statements: tableDropStatements,
		})
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(strings.Join(tableDropStatements, "; "))}
		}
		if err := op.Wait(ctx); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(strings.Join(tableDropStatements, "; "))}
		}
	}

	return nil
}

func (s *Spanner) viewDropStatements(ctx context.Context) ([]string, error) {
		dropViewsIter := s.db.data.Single().Query(ctx, spanner.NewStatement(`SELECT
  		CONCAT('DROP VIEW `+"`', table_name, '`') AS ddl"+`
	FROM information_schema.tables
	WHERE table_schema = ''
  		AND table_type = 'VIEW'
	ORDER BY table_name;`))
	defer dropViewsIter.Stop()

	stmts := make([]string, 0)
	for {
		row, err := dropViewsIter.Next()
		if err == iterator.Done {
			break
		}
		var stmt string
		if err := row.Columns(&stmt); err != nil {
			return nil, &database.Error{OrigErr: err}
		}

		stmts = append(stmts, stmt)

	}

	return stmts, nil
}

func (s *Spanner) constraintDropStatements(ctx context.Context) ([]string, error) {
		dropConstraintsIter := s.db.data.ReadOnlyTransaction().Query(ctx, spanner.NewStatement(`SELECT
		CONCAT( 'ALTER TABLE ',
			CASE
			WHEN tc.table_schema = '' THEN CONCAT('`+"`', tc.table_name, '`')"+`
			ELSE CONCAT('`+"`', tc.table_schema, '`.`', tc.table_name, '`')"+`
		END
			, ' DROP CONSTRAINT `+"`', tc.constraint_name, '`' ) AS ddl"+`
		FROM
		information_schema.table_constraints tc
		WHERE
		tc.constraint_type = 'FOREIGN KEY'
		ORDER BY
		tc.table_schema,
		tc.table_name,
		tc.constraint_name;`))
	defer dropConstraintsIter.Stop()

	stmts := make([]string, 0)
	for {
		row, err := dropConstraintsIter.Next()
		if err == iterator.Done {
			break
		}
		var stmt string
		if err := row.Columns(&stmt); err != nil {
			return nil, &database.Error{OrigErr: err}
		}

		stmts = append(stmts, stmt)

	}

	return stmts, nil
}

func (s *Spanner) tableDropStatements(ctx context.Context) ([]string, error) {
		dropTablesIter := s.db.data.ReadOnlyTransaction().Query(ctx, spanner.NewStatement(`WITH t AS (
  	SELECT table_name, parent_table_name
  FROM information_schema.tables
  WHERE table_schema = ''
    AND table_type = 'BASE TABLE'
),
d AS (
  SELECT
    c.table_name,
    CAST(p1.table_name IS NOT NULL AS INT64) +
    CAST(p2.table_name IS NOT NULL AS INT64) +
    CAST(p3.table_name IS NOT NULL AS INT64) +
    CAST(p4.table_name IS NOT NULL AS INT64) +
    CAST(p5.table_name IS NOT NULL AS INT64) +
    CAST(p6.table_name IS NOT NULL AS INT64) +
    CAST(p7.table_name IS NOT NULL AS INT64) AS depth
  FROM t c
  LEFT JOIN t p1 ON c.parent_table_name = p1.table_name
  LEFT JOIN t p2 ON p1.parent_table_name = p2.table_name
  LEFT JOIN t p3 ON p2.parent_table_name = p3.table_name
  LEFT JOIN t p4 ON p3.parent_table_name = p4.table_name
  LEFT JOIN t p5 ON p4.parent_table_name = p5.table_name
  LEFT JOIN t p6 ON p5.parent_table_name = p6.table_name
  LEFT JOIN t p7 ON p6.parent_table_name = p7.table_name
)
SELECT CONCAT('DROP TABLE `+"`', table_name, '`') AS ddl"+`
FROM d
ORDER BY depth DESC, table_name;`))
	defer dropTablesIter.Stop()

	stmts := make([]string, 0)
	for {
		row, err := dropTablesIter.Next()
		if err == iterator.Done {
			break
		}
		var stmt string
		if err := row.Columns(&stmt); err != nil {
			return nil, &database.Error{OrigErr: err}
		}

		stmts = append(stmts, stmt)

	}

	return stmts, nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Spanner type.
func (s *Spanner) ensureVersionTable() (err error) {
	if err = s.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := s.Unlock(); e != nil {
			err = errors.Join(err, e)
		}
	}()

	ctx := context.Background()
	tbl := s.config.MigrationsTable
	iter := s.db.data.Single().Read(ctx, tbl, spanner.AllKeys(), []string{"Version"})
	if err := iter.Do(func(r *spanner.Row) error { return nil }); err == nil {
		return nil
	}

	stmt := fmt.Sprintf(`CREATE TABLE %s (
    Version INT64 NOT NULL,
    Dirty    BOOL NOT NULL
	) PRIMARY KEY(Version)`, tbl)

	op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: []string{stmt},
	})
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}
	if err := op.Wait(ctx); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}

	return nil
}

type statementType string

const (
	statementTypeUnknown statementType = ""
	statementTypeDDL     statementType = "DDL"
	statementTypeDML     statementType = "DML"
)

type statementGroup struct {
	typ   statementType
	stmts []string
}

func statementGroups(migr []byte) (groups []*statementGroup, err error) {
	lex := &memefish.Lexer{
		File: &token.File{Buffer: string(migr)},
	}

	group := &statementGroup{}
	var stmtTyp statementType
	var stmt strings.Builder
	for {
		if err := lex.NextToken(); err != nil {
			return nil, err
		}

		if stmtTyp == statementTypeUnknown {
			switch {
			case lex.Token.IsKeywordLike("INSERT") || lex.Token.IsKeywordLike("DELETE") || lex.Token.IsKeywordLike("UPDATE"):
				stmtTyp = statementTypeDML
			default:
				stmtTyp = statementTypeDDL
			}
			if group.typ != stmtTyp {
				if len(group.stmts) > 0 {
					groups = append(groups, group)
				}
				group = &statementGroup{typ: stmtTyp}
			}
		}

		if lex.Token.Kind == token.TokenEOF || lex.Token.Kind == ";" {
			if stmt.Len() > 0 {
				group.stmts = append(group.stmts, stmt.String())
			}
			stmtTyp = statementTypeUnknown
			stmt.Reset()

			if lex.Token.Kind == token.TokenEOF {
				if len(group.stmts) > 0 {
					groups = append(groups, group)
				}

				break
			}

			continue
		}

		if len(lex.Token.Comments) > 0 && strings.HasPrefix(lex.Token.Comments[0].Raw, "--") {
			// standard comment Token consumes a \n, so we need to add it back
			if _, err := stmt.WriteString("\n"); err != nil {
				return nil, err
			}
		}
		if stmt.Len() > 0 {
			if _, err := stmt.WriteString(lex.Token.Space); err != nil {
				return nil, err
			}
		}
		if _, err := stmt.WriteString(lex.Token.Raw); err != nil {
			return nil, err
		}
	}

	return groups, nil
}
