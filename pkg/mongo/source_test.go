package mongo

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/turbolytics/librarian/pkg/replicator"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func TestIntegrationMongoSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	// Start MongoDB container with replica set enabled
	// Wait for MongoDB to be ready to accept connections
	mongoContainer, err := mongodb.Run(ctx,
		"mongo:6",
		mongodb.WithReplicaSet("rs0"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Waiting for connections").
				WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := mongoContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate mongoContainer: %s", err)
		}
	})

	// Get connection string
	connStr, err := mongoContainer.ConnectionString(ctx)
	require.NoError(t, err)

	// The testcontainers MongoDB module automatically initializes the replica set
	// Wait for the replica set to be ready by checking if we can connect
	// Use SetDirect(true) to bypass replica set discovery and connect directly to the host
	tempClient, err := mongo.Connect(ctx, options.Client().
		ApplyURI(connStr).
		SetDirect(true))
	require.NoError(t, err)

	// Poll for replica set to be ready by checking for a PRIMARY member
	deadline := time.Now().Add(30 * time.Second)
	var isReady bool
	for time.Now().Before(deadline) {
		var status bson.M
		err := tempClient.Database("admin").RunCommand(ctx, bson.D{
			{Key: "replSetGetStatus", Value: 1},
		}).Decode(&status)

		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Check if we have a primary
		if members, ok := status["members"].(bson.A); ok {
			for _, member := range members {
				if m, ok := member.(bson.M); ok {
					if stateStr, ok := m["stateStr"].(string); ok && stateStr == "PRIMARY" {
						isReady = true
						break
					}
				}
			}
		}

		if isReady {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	tempClient.Disconnect(ctx)
	require.True(t, isReady, "replica set failed to elect a primary within timeout")

	// Parse the connection string and add database and collection
	dbName := "testdb"
	collectionName := "testcollection"

	// Parse and modify the URI to include database, collection, and directConnection=true
	// directConnection=true bypasses replica set discovery and connects to the specified host
	mongoURI := fmt.Sprintf("%s/%s?collection=%s&directConnection=true", connStr, dbName, collectionName)
	parsedURI, err := url.Parse(mongoURI)
	require.NoError(t, err)

	// Create logger
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// Create MongoDB source
	source, err := NewSource(ctx, parsedURI, logger)
	require.NoError(t, err)

	// Create a client for test operations
	// Use SetDirect(true) to bypass replica set discovery
	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI(connStr).
		SetDirect(true))
	require.NoError(t, err)
	defer client.Disconnect(ctx)

	// Get the collection
	coll := client.Database(dbName).Collection(collectionName)

	// Connect the source (this starts the change stream)
	err = source.Connect(ctx, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := source.Disconnect(ctx); err != nil {
			t.Logf("failed to disconnect source: %s", err)
		}
	})

	// Insert a test document
	testDoc := bson.M{
		"_id":   "test-id-123",
		"name":  "John Doe",
		"email": "john@example.com",
		"age":   30,
	}

	insertResult, err := coll.InsertOne(ctx, testDoc)
	require.NoError(t, err)
	require.Equal(t, "test-id-123", insertResult.InsertedID)

	// Read the change event with timeout
	eventChan := make(chan replicator.Event, 1)
	errChan := make(chan error, 1)

	go func() {
		event, err := source.Next(ctx)
		if err != nil {
			errChan <- err
			return
		}
		eventChan <- event
	}()

	// Wait for event or timeout
	select {
	case event := <-eventChan:
		// Assert the event
		assert.Equal(t, replicator.OpCreate, event.Payload.Op, "operation should be create")
		assert.Equal(t, "mongodb", event.Payload.Source.Connector, "connector should be mongodb")
		assert.Equal(t, dbName, event.Payload.Source.Db, "database should match")
		assert.Equal(t, collectionName, event.Payload.Source.Table, "collection should match")

		// Check the after data
		require.NotNil(t, event.Payload.After, "after data should not be nil")
		assert.Equal(t, "test-id-123", event.Payload.After["_id"], "_id should match")
		assert.Equal(t, "John Doe", event.Payload.After["name"], "name should match")
		assert.Equal(t, "john@example.com", event.Payload.After["email"], "email should match")
		assert.Equal(t, int32(30), event.Payload.After["age"], "age should match")

		// Check that before data is nil for insert
		assert.Nil(t, event.Payload.Before, "before data should be nil for insert")

		// Check that position is set (resume token)
		assert.NotEmpty(t, event.Position, "position should be set")

	case err := <-errChan:
		t.Fatalf("failed to read event: %v", err)

	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for change event")
	}

	// Verify source stats
	stats := source.Stats()
	assert.True(t, stats.ConnectionHealthy, "connection should be healthy")
	assert.Equal(t, int64(1), stats.TotalEvents, "should have received 1 event")
	assert.Greater(t, stats.TotalBytes, int64(0), "should have received some bytes")
}
