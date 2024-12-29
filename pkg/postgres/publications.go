package postgres

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
)

var extractPublicationNameRe = regexp.MustCompile(`CREATE PUBLICATION\s+(\w+)`)

func ExtractPulicationNameFromQuery(query string) (string, error) {
	matches := extractPublicationNameRe.FindStringSubmatch(query)
	if len(matches) < 2 {
		return "", fmt.Errorf("publication name not found")
	}

	// Return the publication name (matches[1] contains the first captured group)
	return matches[1], nil
}

func PublicationExists(ctx context.Context, conn *pgx.Conn, name string) (bool, error) {
	var exists bool
	err := conn.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM pg_publication
    WHERE pubname = $1
)`, name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check if publication exists: %w", err)
	}

	return exists, nil
}
