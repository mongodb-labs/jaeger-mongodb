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

// var ip_address = "192.168.1.197"

var ip_address = "10.1.15.231"

func TestTagFiltering(t *testing.T) {
	m, err := mongo.Connect(context.TODO(), options.Client().
		ApplyURI(fmt.Sprintf("mongodb://%s:27017", ip_address)).
		SetWriteConcern(writeconcern.New(writeconcern.W(1))))
	if err != nil {
		log.Fatal(err)
	}
	reader := jager_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces"), nil)

	ctx := context.TODO()
	fmt.Println(reader.GetOperations(ctx, spanstore.OperationQueryParameters{}))
	fmt.Println(time.Unix(1623086597, 0))
	fmt.Println("----")

	testCases := []struct {
		name         string
		tags         map[string]string
		runAssertion func(map[string]string)
	}{
		{
			name: "All customer service traces with status code 200",
			tags: map[string]string{
				"http.status_code": "200",
			},
			runAssertion: func(tags_in map[string]string) {
				tracesOutput, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
					StartTimeMin: time.Date(1990, 4, 30, 1, 30, 0, 0, time.UTC),
					StartTimeMax: time.Now(),
					Tags:         tags_in,
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
			},
		},
		{
			name: "All jaeger-query service traces with status_code=404, component='custom-component' and internal.span.format='custom-format'",
			tags: map[string]string{
				"http.status_code":     "200",
				"component":            "custom-component",
				"internal.span.format": "custom-format",
			},
			runAssertion: func(tags_in map[string]string) {
				tracesOutput, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
					StartTimeMin: time.Date(1990, 4, 30, 1, 30, 0, 0, time.UTC),
					StartTimeMax: time.Now(),
					Tags:         tags_in,
					ServiceName:  "jaeger-query",
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
							} else if tag.Key == "component" {
								assert.Equal(t, "custom-component", tag.VStr, "Component should be 'custom-component'")
							} else if tag.Key == "internal.span.format" {
								assert.Equal(t, "custom-format", tag.VStr, "internal span format should be 'custom-format' ")
							}
						}
					}
				}
				assert.Len(t, tracesOutput, 0, "There should only be one trace that matches the tag filtering query condition")
			},
		},
	}

	for _, tc := range testCases {
		println(tc.name)
		tc.runAssertion(tc.tags)
		println("====")
	}
}
