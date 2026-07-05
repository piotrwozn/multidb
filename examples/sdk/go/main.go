package main

import (
	"context"
	"fmt"
	"os"
	"time"

	multidb "github.com/multidb/multidb/sdk/go"
)

func main() {
	ctx := context.Background()
	baseURL := getenv("MULTIDB_CONTROL_PLANE_URL", "http://127.0.0.1:8080/api")
	password := getenv("MULTIDB_ADMIN_PASSWORD", "local-dev-admin-password")
	stamp := fmt.Sprintf("go_%d", time.Now().UnixMilli())

	client := multidb.NewClient(multidb.WithBaseURL(baseURL))
	session, err := client.Login(ctx, "admin", password)
	must(err)
	db := client.WithToken(session.Token)
	defer func() { _, _ = db.Logout(ctx) }()

	table := "sdk_users_" + stamp
	_, err = db.CreateTable(ctx, multidb.JSONObject{
		"name": table,
		"schema": multidb.JSONObject{
			"columns": []any{
				multidb.JSONObject{"name": "id", "ty": "Int", "nullable": false},
				multidb.JSONObject{"name": "name", "ty": "Str", "nullable": false},
			},
			"primary_key": 0,
		},
		"indexes": []any{},
	})
	must(err)
	_, err = db.InsertTableRow(ctx, table, []multidb.JSONValue{1, "Ada"})
	must(err)
	_, err = db.SQL(ctx, "SELECT * FROM "+table)
	must(err)

	collection := "sdk_docs_" + stamp
	_, err = db.CreateCollection(ctx, multidb.JSONObject{
		"name": collection,
		"fields": []any{
			multidb.JSONObject{"name": "name", "source": multidb.JSONObject{"Path": []any{"name"}}, "ty": "Str"},
		},
		"indexes": []any{},
	})
	must(err)
	_, err = db.CreateDocument(ctx, collection, multidb.JSONObject{"name": "Ada"})
	must(err)

	vectors := "sdk_vectors_" + stamp
	_, err = db.CreateVector(ctx, multidb.JSONObject{"name": vectors, "dim": 3})
	must(err)
	_, err = db.InsertVector(ctx, vectors, multidb.JSONObject{"label": "Ada"}, []float64{1, 0, 0})
	must(err)
	_, err = db.SearchVector(ctx, vectors, []float64{1, 0, 0}, 1)
	must(err)

	series := "sdk_series_" + stamp
	_, err = db.CreateTimeSeries(ctx, multidb.JSONObject{"name": series, "chunk_millis": 60000, "retention_millis": nil})
	must(err)
	now := time.Now().UnixMilli()
	_, err = db.InsertTimeSeriesPoint(ctx, series, "default", multidb.JSONObject{"timestamp_millis": now, "value": 42.0})
	must(err)
	_, err = db.TimeSeriesPoints(ctx, series, "default", now-1, now+1)
	must(err)

	fmt.Println("Go SDK example completed")
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
