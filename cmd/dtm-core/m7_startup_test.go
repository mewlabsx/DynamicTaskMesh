package main

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	sqliteplatform "dtm/internal/platform/sqlite"

	_ "modernc.org/sqlite"
)

func TestM7StorageStartupExitCodes(t *testing.T) {
	for _, test := range []struct {
		name      string
		prepare   func(*testing.T, string)
		path      func(string) string
		wantCode  int
		wantClass string
	}{
		{name: "path", prepare: func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "block"), []byte("file"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, path: func(root string) string { return filepath.Join(root, "block", "core.db") }, wantCode: 10, wantClass: "path"},
		{name: "corrupt", prepare: func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "core.db"), []byte("not sqlite"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, path: func(root string) string { return filepath.Join(root, "core.db") }, wantCode: 13, wantClass: "corrupt"},
		{name: "schema", prepare: prepareM7CoreChecksumMismatch, path: func(root string) string { return filepath.Join(root, "core.db") }, wantCode: 12, wantClass: "schema_mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			test.prepare(t, root)
			databasePath := test.path(root)
			configPath := filepath.Join(root, "core.yaml")
			writeCoreConfigAt(t, configPath, "server:\n  address: 127.0.0.1:0\nstorage:\n  path: "+strconv.Quote(databasePath)+"\n")
			var stdout, stderr bytes.Buffer
			if code := run(context.Background(), []string{"-config", configPath}, &stdout, &stderr); code != test.wantCode {
				t.Fatalf("run() code=%d want=%d stdout=%q stderr=%q", code, test.wantCode, stdout.String(), stderr.String())
			}
			if stdout.Len() != 0 || !strings.Contains(stderr.String(), "event=storage_startup_failed") || !strings.Contains(stderr.String(), "error_class="+test.wantClass) {
				t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
			for _, forbidden := range []string{"SELECT ", "INSERT ", "SQLite", "SQL logic"} {
				if strings.Contains(stderr.String(), forbidden) {
					t.Fatalf("startup stderr leaked %q: %q", forbidden, stderr.String())
				}
			}
		})
	}
}

func prepareM7CoreChecksumMismatch(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, "core.db")
	repository, err := sqliteplatform.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = repository.Close()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE schema_migrations SET checksum='mismatch' WHERE version=1`); err != nil {
		t.Fatal(err)
	}
}
