package jager_mongodb_test

import (
	"context"
	"fmt"
	jager_mongodb "jaeger-mongodb/internal/jager-mongodb"
	"log"
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/stretchr/testify/assert"
)

func TestTagFiltering(t *testing.T) {
	m, err := mongo.Connect(context.TODO(), options.Client().
		ApplyURI("mongodb://10.1.15.231:27017").
		SetWriteConcern(writeconcern.New(writeconcern.W(1))))
	if err != nil {
		log.Fatal(err)
	}
	reader := jager_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces"), nil)
	//fmt.Println(r.GetServices(context.TODO()))
	//fmt.Println(r.GetOperations(context.TODO(), spanstore.OperationQueryParameters{
	//		ServiceName: "frontend",
	//}))
	tags := map[string]string{
		"http.status_code": "200",
	}

	ctx := context.TODO()
	fmt.Println(reader.GetOperations(ctx, spanstore.OperationQueryParameters{}))
	fmt.Println(time.Unix(1623086597, 0))
	fmt.Println("----")
	tracesOutput, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
		StartTimeMin: time.Date(1990, 4, 30, 1, 30, 0, 0, time.UTC),
		StartTimeMax: time.Now(),
		Tags:         tags,
		ServiceName:  "customer",
		NumTraces:    100,
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, trace := range tracesOutput {
		for _, s := range trace.Spans {
			for _, tag := range s.Tags {
				if tag.Key == "http.status_code" {
					assert.Equal(t, int64(200), tag.VInt64, "Status code should be 200")
				}
			}
		}
	}
}
