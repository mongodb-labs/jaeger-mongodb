package jaeger_mongodb_test

import (
	"context"
	"fmt"
	jaeger_mongodb "jaeger-mongodb/internal/jaeger-mongodb"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/stretchr/testify/assert"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.uber.org/zap"
)

const ip_address = "10.1.15.231"

var dummyKv []model.KeyValue = []model.KeyValue{
	{
		Key:    "http.status_code",
		VType:  model.Int64Type,
		VInt64: 200,
	},
}

var statusCode404 []model.KeyValue = []model.KeyValue{
	{
		Key:    "http.status_code",
		VType:  model.Int64Type,
		VInt64: 404,
	},
}

// Helper function to verify depedency pattern is valid.
func isValidDepedencyPattern(pattern string) bool {
	switch pattern {
	case
		"none",
		"single",
		"circular":
		return true
	}
	return false
}

// Helper function to generate traces for testing purposes.
func GenerateTraces(ctx context.Context, writer *jaeger_mongodb.SpanWriter, numTraces int, dependencyPattern string, followsFrom bool) {
	if !isValidDepedencyPattern(dependencyPattern) {
		zap.S().Fatal(fmt.Printf("Error: Dependency pattern '%s' does not exist!\n", dependencyPattern))
	}
	randomOperation := map[int]string{0: "grpc", 1: "http", 2: "spark", 3: "redis"}
	for i := 0; i < numTraces; i++ {
		tags := dummyKv
		if i%10 == 0 {
			tags = statusCode404
		}
		s := model.Span{
			TraceID:       model.NewTraceID(uint64(i), uint64(i)),
			SpanID:        model.NewSpanID(uint64(i)),
			OperationName: randomOperation[i%4],
			References:    []model.SpanRef{},
			StartTime:     time.Now().Add(time.Duration(i)),
			Duration:      time.Duration(i + 10),
			Tags:          tags,
			Process: &model.Process{
				ServiceName: fmt.Sprintf("Service %d", i),
				Tags:        dummyKv,
			},
			Logs: []model.Log{
				{
					Timestamp: time.Now(),
					Fields:    dummyKv,
				},
			},
		}
		if dependencyPattern == "circular" && i < numTraces-1 {
			newRef := model.NewChildOfRef(model.TraceID{Low: uint64(i + 1), High: uint64(i + 1)}, model.SpanID(uint64(i+1)))
			if followsFrom {
				newRef = model.NewFollowsFromRef(model.TraceID{Low: uint64(i + 1), High: uint64(i + 1)}, model.SpanID(uint64(i+1)))
			}
			s.References = append(s.References, newRef)
		}
		if (dependencyPattern == "single" || dependencyPattern == "circular") && i > 0 {
			newRef := model.NewChildOfRef(model.TraceID{Low: uint64(i - 1), High: uint64(i - 1)}, model.SpanID(uint64(i-1)))
			if followsFrom {
				newRef = model.NewFollowsFromRef(model.TraceID{Low: uint64(i - 1), High: uint64(i - 1)}, model.SpanID(uint64(i-1)))
			}
			s.References = append(s.References, newRef)
		}
		writer.WriteSpan(ctx, &s)
	}
}
func TestReaderIntegration(t *testing.T) {
	m, err := mongo.Connect(context.TODO(), options.Client().
		ApplyURI(fmt.Sprintf("mongodb://%s:27017", ip_address)).
		SetWriteConcern(writeconcern.New(writeconcern.W(1))))

	if err != nil {
		log.Fatal(err)
	}

	twentyYears, err := time.ParseDuration("175200h")
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func() {
		if err = m.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	testCases := []struct {
		name         string
		endTs        time.Time
		lookback     time.Duration
		runAssertion func(time.Time, time.Duration)
	}{
		{
			name:     "Test GetDependencies -- single depedency",
			endTs:    time.Date(2030, 9, 2, 7, 30, 15, 100, time.UTC),
			lookback: twentyYears,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				// Clear database to ensure idempotency.
				m.Database("jaeger-tracing").Collection("traces1").Drop(ctx)
				reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces1"), nil)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing").Collection("traces1"), nil)
				// Generate single dependency traces
				GenerateTraces(ctx, writer, 100, "single", false)
				dls, err := reader.GetDependencies(ctx, endTs, lookback)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("Length of dls: %d\n", len(dls))
				assert.Equal(t, 99, len(dls), "number of dependency links should be 99")
				for _, dl := range dls {
					parentInt, err := strconv.Atoi(strings.Split(dl.GetParent(), " ")[1])
					if err != nil {
						log.Fatal(err)
					}
					childInt, err := strconv.Atoi(strings.Split(dl.GetChild(), " ")[1])
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf("Parent: %d, Child: %d, Call Count: %d\n", parentInt, childInt, dl.GetCallCount())
					assert.Equal(t, parentInt+1, childInt, "parent must be 1 less than child")
					assert.Equal(t, uint64(1), dl.CallCount)
				}
			},
		},
		{
			name:     "Test GetDependencies -- circular dependency",
			endTs:    time.Date(2030, 9, 2, 7, 30, 15, 100, time.UTC),
			lookback: twentyYears,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				// Clear collection to ensure idempotency.
				m.Database("jaeger-tracing").Collection("traces2").Drop(ctx)
				reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces2"), nil)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing").Collection("traces2"), nil)
				// Generate traces with circular dependencies
				GenerateTraces(ctx, writer, 50, "circular", false)
				dls, err := reader.GetDependencies(ctx, endTs, lookback)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("Length of dls: %d\n", len(dls))
				assert.Equal(t, 98, len(dls), "number of dependency links should be 98")
				for _, dl := range dls {
					parentInt, err := strconv.Atoi(strings.Split(dl.GetParent(), " ")[1])
					if err != nil {
						log.Fatal(err)
					}
					childInt, err := strconv.Atoi(strings.Split(dl.GetChild(), " ")[1])
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf("Parent: %d, Child: %d, Call Count: %d\n", parentInt, childInt, dl.GetCallCount())
					assert.Contains(t, [2]int{parentInt + 1, parentInt - 1}, childInt)
					assert.Equal(t, uint64(1), dl.CallCount)
				}
			},
		},
		{
			name:     "Test GetDependencies -- empty trace period",
			endTs:    time.Date(2030, 9, 2, 7, 30, 15, 100, time.UTC),
			lookback: time.Duration(time.Hour.Hours() * 1),
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces3"), nil)
				dls, err := reader.GetDependencies(ctx, endTs, lookback)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("Length of dls: %d\n", len(dls))
				assert.Equal(t, 0, len(dls), "number of dependency links should be 0")
			},
		},
		{
			name:     "Test GetDependencies -- ensure depedency are not transitive",
			endTs:    time.Date(2030, 9, 2, 7, 30, 15, 100, time.UTC),
			lookback: twentyYears,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				// Clear database to ensure idempotency.
				m.Database("jaeger-tracing").Collection("traces4").Drop(ctx)
				reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces4"), nil)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing").Collection("traces4"), nil)
				GenerateTraces(ctx, writer, 3, "single", false)
				dls, err := reader.GetDependencies(ctx, endTs, lookback)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("Length of dls: %d\n", len(dls))
				assert.Equal(t, 2, len(dls), "number of dependency links should be 2")
				for _, dl := range dls {
					parentInt, err := strconv.Atoi(strings.Split(dl.GetParent(), " ")[1])
					if err != nil {
						log.Fatal(err)
					}
					childInt, err := strconv.Atoi(strings.Split(dl.GetChild(), " ")[1])
					if err != nil {
						log.Fatal(err)
					}
					assert.Equal(t, uint64(1), dl.CallCount)
					// Ensure that all the dependency are for contiguous nodes.
					// For instance, A -> B -> C does not imply A -> C.
					assert.Equal(t, 1, childInt-parentInt)
				}
			},
		},
		{
			name:     "Test GetDependencies -- follow from relationship",
			endTs:    time.Date(2030, 9, 2, 7, 30, 15, 100, time.UTC),
			lookback: twentyYears,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				// Clear database to ensure idempotency.
				m.Database("jaeger-tracing").Collection("traces5").Drop(ctx)
				reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces5"), nil)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing").Collection("traces5"), nil)
				GenerateTraces(ctx, writer, 100, "circular", true)
				dls, err := reader.GetDependencies(ctx, endTs, lookback)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("Length of dls: %d\n", len(dls))
				assert.Equal(t, 0, len(dls), "number of dependency links should be 0")
			},
		},
		{
			name:     "Test GetServices",
			endTs:    time.Date(2030, 9, 2, 7, 30, 15, 100, time.UTC),
			lookback: twentyYears,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				// Clear database to ensure idempotency.
				m.Database("jaeger-tracing").Collection("traces6").Drop(ctx)
				reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces6"), nil)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing").Collection("traces6"), nil)
				GenerateTraces(ctx, writer, 50, "circular", false)
				ops, err := reader.GetServices(ctx)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("Length of ops: %d\n", len(ops))
				assert.Equal(t, 50, len(ops), "number of services should be 50")
				for _, s := range ops {
					fmt.Printf("Service %s \n", s)
					arr := strings.Split(s, " ")
					name, num := arr[0], arr[1]
					numInt, err := strconv.Atoi(num)
					if err != nil {
						log.Fatal(err)
					}
					assert.Equal(t, "Service", name)
					assert.True(t, 0 <= numInt && numInt < 50)
				}
			},
		},
		{
			name:     "Test GetOperations",
			endTs:    time.Date(2030, 9, 2, 7, 30, 15, 100, time.UTC),
			lookback: twentyYears,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				// Clear database to ensure idempotency.
				m.Database("jaeger-tracing").Collection("traces7").Drop(ctx)
				reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces7"), nil)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing").Collection("traces7"), nil)
				GenerateTraces(ctx, writer, 50, "circular", false)
				ops, err := reader.GetOperations(ctx, spanstore.OperationQueryParameters{})
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("Length of operation: %d\n", len(ops))
				assert.True(t, 0 <= len(ops) && len(ops) <= 4, "number of operation should be less than or equal to 4")
				for _, s := range ops {
					fmt.Printf("Operation: %s\n", s.Name)
					assert.Contains(t, [4]string{"grpc", "http", "spark", "redis"}, s.Name)
				}
			},
		},
		{
			name:     "Test GetTrace",
			endTs:    time.Date(2030, 9, 2, 7, 30, 15, 100, time.UTC),
			lookback: twentyYears,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				// Clear database to ensure idempotency.
				m.Database("jaeger-tracing").Collection("traces8").Drop(ctx)
				reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces8"), nil)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing").Collection("traces8"), nil)
				GenerateTraces(ctx, writer, 50, "single", false)
				for i := 0; i < 50; i++ {
					trace, err := reader.GetTrace(ctx, model.TraceID{High: uint64(i), Low: uint64(i)})
					if err != nil {
						log.Fatal(err)
					}
					assert.Equal(t, fmt.Sprintf("Service %d", i), trace.GetSpans()[0].Process.ServiceName)
				}
			},
		},
		{
			name:     "Test Find Traces",
			endTs:    time.Date(2030, 9, 2, 7, 30, 15, 100, time.UTC),
			lookback: twentyYears,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				// Clear database to ensure idempotency.
				m.Database("jaeger-tracing").Collection("traces9").Drop(ctx)
				reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces9"), nil)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing").Collection("traces9"), nil)
				GenerateTraces(ctx, writer, 50, "single", false)
				traces, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
					StartTimeMin: time.Date(1997, 04, 30, 05, 1, 1, 1, time.UTC),
					StartTimeMax: time.Now(),
					NumTraces:    1500,
				})
				if err != nil {
					log.Fatal(err)
				}
				// Ensure we have 50 traces.
				assert.Equal(t, 50, len(traces))
				// Ensure all the traces have only 1 span.
				for _, trace := range traces {
					assert.Equal(t, 1, len(trace.GetSpans()))
				}
			},
		},
		{
			name:     "Test Find Traces -- Tag Filtering",
			endTs:    time.Date(2030, 9, 2, 7, 30, 15, 100, time.UTC),
			lookback: twentyYears,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				// Clear database to ensure idempotency.
				m.Database("jaeger-tracing").Collection("traces10").Drop(ctx)
				reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces10"), nil)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing").Collection("traces10"), nil)
				GenerateTraces(ctx, writer, 50, "circular", false)
				traces200, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
					StartTimeMin: time.Date(1997, 04, 30, 05, 1, 1, 1, time.UTC),
					StartTimeMax: time.Now(),
					NumTraces:    1500,
					Tags: map[string]string{
						"http.status_code": "200",
					},
				})
				if err != nil {
					log.Fatal(err)
				}
				// Assert there are 45 traces with tag -- http.status_code = 200
				assert.Equal(t, 45, len(traces200))
				traces404, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
					StartTimeMin: time.Date(1997, 04, 30, 05, 1, 1, 1, time.UTC),
					StartTimeMax: time.Now(),
					NumTraces:    1500,
					Tags: map[string]string{
						"http.status_code": "404",
					},
				})
				if err != nil {
					log.Fatal(err)
				}
				// Assert there are only 5 traces with tag -- http.status_code = 404
				assert.Equal(t, 5, len(traces404))
			},
		},
	}
	for _, tc := range testCases {
		println(tc.name)
		tc.runAssertion(tc.endTs, tc.lookback)
		println("====")
	}
}
func BenchmarkTagFiltering(b *testing.B) {
	m, err := mongo.Connect(context.TODO(), options.Client().
		ApplyURI(fmt.Sprintf("mongodb://%s:27017", ip_address)).
		SetWriteConcern(writeconcern.New(writeconcern.W(1))))
	if err != nil {
		log.Fatal(err)
	}
	reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces"), nil)

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
			name: "Benchmark test for tag filtering with status code 200",
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
								assert.Equal(b, int64(200), tag.VInt64, "Status code should be 200")
							}
						}
					}
				}
			},
		},
	}
	for _, tc := range testCases {
		println(tc.name)
		tc.runAssertion(tc.tags)
		println("====")
	}
}

func TestTagFiltering(t *testing.T) {
	m, err := mongo.Connect(context.TODO(), options.Client().
		ApplyURI(fmt.Sprintf("mongodb://%s:27017", ip_address)).
		SetWriteConcern(writeconcern.New(writeconcern.W(1))))
	if err != nil {
		log.Fatal(err)
	}
	reader := jaeger_mongodb.NewSpanReader(m.Database("jaeger-tracing").Collection("traces"), nil)

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
					NumTraces:    1500,
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
