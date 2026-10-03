package storage

import (
	"fmt"
	"io"
	"strconv"
)

func WriteStartupFailure(writer io.Writer, program, databasePath string, err error) int {
	code := StartupExitCode(err)
	class := ClassifyError(err)
	if class == ErrorClassUnknown {
		return code
	}
	fmt.Fprintf(writer, "event=storage_startup_failed program=%s operation=open error_class=%s exit_code=%d database_path=%s\n", program, class, code, strconv.Quote(databasePath))
	return code
}
