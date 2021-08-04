package jaeger_mongodb_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/stretchr/testify/assert"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.uber.org/zap"

	jaeger_mongodb "jaeger-mongodb/internal/jaeger-mongodb"
	mock "jaeger-mongodb/mocks"
)

var spanTagsSuccess []model.KeyValue = []model.KeyValue{
	{
		Key:    "http.status_code",
		VType:  model.Int64Type,
		VInt64: 200,
	},
}

var spanTagsFailure []model.KeyValue = []model.KeyValue{
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

func createNewCollectionName(uniqueCollectionName map[string]int) string {
	collectionName := fmt.Sprintf("traces%d", rand.Int())
	// Ensure collectionName is unique
	for {
		_, found := uniqueCollectionName[collectionName]
		if !found {
			break
		}
		collectionName = fmt.Sprintf("traces%d", rand.Int())
	}
	return collectionName
}

// Helper function to generate traces for testing purposes.
func generateTraces(ctx context.Context, writer *jaeger_mongodb.SpanWriter, numTraces int, dependencyPattern string, followsFrom bool) {
	if !isValidDepedencyPattern(dependencyPattern) {
		zap.S().Fatal(fmt.Printf("Error: Dependency pattern '%s' does not exist!\n", dependencyPattern))
	}
	randomOperation := map[int]string{0: "grpc", 1: "http", 2: "spark", 3: "redis"}
	for i := 0; i < numTraces; i++ {
		tags := spanTagsSuccess
		if i%10 == 0 {
			tags = spanTagsFailure
		}
		s := model.Span{
			TraceID:       model.NewTraceID(uint64(i), uint64(i)),
			SpanID:        model.NewSpanID(uint64(i)),
			OperationName: randomOperation[i%4],
			References:    []model.SpanRef{},
			StartTime:     time.Date(2021, 7, 1, 1, 1, 1, 1, time.UTC),
			Duration:      time.Duration(i + 10),
			Tags:          tags,
			Process: &model.Process{
				ServiceName: fmt.Sprintf("Service %d", i),
				Tags:        spanTagsSuccess,
			},
			Logs: []model.Log{
				{
					Timestamp: time.Now(),
					Fields:    spanTagsSuccess,
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
	mongoURL := os.Getenv("MONGO_URL")
	if mongoURL == "" {
		t.Skip("set MONGO_URL to run the IT tests")
	}
	m, err := mongo.Connect(context.TODO(), options.Client().
		ApplyURI(mongoURL).
		SetWriteConcern(writeconcern.New(writeconcern.W(1))))

	if err != nil {
		t.Error(err)
	}

	fourteenDays, err := time.ParseDuration("336h")
	if err != nil {
		t.Error(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func() {
		if err = m.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	// Map to ensure no duplicated collection names are used.
	uniqueCollectionName := map[string]int{}
	testCases := []struct {
		name         string
		endTs        time.Time
		lookback     time.Duration
		runAssertion func(time.Time, time.Duration)
	}{
		{
			name:     "Test GetDependencies -- single depedency",
			endTs:    time.Date(2021, 7, 2, 1, 1, 1, 1, time.UTC),
			lookback: fourteenDays,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				collectionName := createNewCollectionName(uniqueCollectionName)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing-test").Collection(collectionName), nil)
				// Generate single dependency traces
				generateTraces(ctx, writer, 100, "single", false)
				dls, err := reader.GetDependencies(ctx, endTs, lookback)
				if err != nil {
					t.Error(err)
				}
				fmt.Printf("Length of dls: %d\n", len(dls))
				assert.Equal(t, 99, len(dls), "number of dependency links should be 99")
				for _, dl := range dls {
					parentInt, err := strconv.Atoi(strings.Split(dl.GetParent(), " ")[1])
					if err != nil {
						t.Error(err)
					}
					childInt, err := strconv.Atoi(strings.Split(dl.GetChild(), " ")[1])
					if err != nil {
						t.Error(err)
					}
					fmt.Printf("Parent: %d, Child: %d, Call Count: %d\n", parentInt, childInt, dl.GetCallCount())
					assert.Equal(t, parentInt+1, childInt, "parent must be 1 less than child")
					assert.Equal(t, uint64(1), dl.CallCount)
				}
			},
		},
		{
			name:     "Test GetDependencies -- circular dependency",
			endTs:    time.Date(2021, 7, 2, 1, 1, 1, 1, time.UTC),
			lookback: fourteenDays,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				collectionName := createNewCollectionName(uniqueCollectionName)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing-test").Collection(collectionName), nil)
				// Generate traces with circular dependencies
				generateTraces(ctx, writer, 50, "circular", false)
				dls, err := reader.GetDependencies(ctx, endTs, lookback)
				if err != nil {
					t.Error(err)
				}
				fmt.Printf("Length of dls: %d\n", len(dls))
				assert.Equal(t, 98, len(dls), "number of dependency links should be 98")
				for _, dl := range dls {
					parentInt, err := strconv.Atoi(strings.Split(dl.GetParent(), " ")[1])
					if err != nil {
						t.Error(err)
					}
					childInt, err := strconv.Atoi(strings.Split(dl.GetChild(), " ")[1])
					if err != nil {
						t.Error(err)
					}
					fmt.Printf("Parent: %d, Child: %d, Call Count: %d\n", parentInt, childInt, dl.GetCallCount())
					assert.Contains(t, [2]int{parentInt + 1, parentInt - 1}, childInt)
					assert.Equal(t, uint64(1), dl.CallCount)
				}
			},
		},
		{
			name:     "Test GetDependencies -- empty trace period",
			endTs:    time.Date(2021, 7, 2, 1, 1, 1, 1, time.UTC),
			lookback: time.Duration(time.Hour.Hours() * 1),
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				collectionName := createNewCollectionName(uniqueCollectionName)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				dls, err := reader.GetDependencies(ctx, endTs, lookback)
				if err != nil {
					t.Error(err)
				}
				fmt.Printf("Length of dls: %d\n", len(dls))
				assert.Equal(t, 0, len(dls), "number of dependency links should be 0")
			},
		},
		{
			name:     "Test GetDependencies -- ensure depedency are not transitive",
			endTs:    time.Date(2021, 7, 2, 1, 1, 1, 1, time.UTC),
			lookback: fourteenDays,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				collectionName := createNewCollectionName(uniqueCollectionName)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing-test").Collection(collectionName), nil)
				generateTraces(ctx, writer, 3, "single", false)
				dls, err := reader.GetDependencies(ctx, endTs, lookback)
				if err != nil {
					t.Error(err)
				}
				fmt.Printf("Length of dls: %d\n", len(dls))
				assert.Equal(t, 2, len(dls), "number of dependency links should be 2")
				for _, dl := range dls {
					parentInt, err := strconv.Atoi(strings.Split(dl.GetParent(), " ")[1])
					if err != nil {
						t.Error(err)
					}
					childInt, err := strconv.Atoi(strings.Split(dl.GetChild(), " ")[1])
					if err != nil {
						t.Error(err)
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
			endTs:    time.Date(2021, 7, 2, 1, 1, 1, 1, time.UTC),
			lookback: fourteenDays,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				collectionName := createNewCollectionName(uniqueCollectionName)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing-test").Collection(collectionName), nil)
				generateTraces(ctx, writer, 100, "circular", true)
				dls, err := reader.GetDependencies(ctx, endTs, lookback)
				if err != nil {
					t.Error(err)
				}
				fmt.Printf("Length of dls: %d\n", len(dls))
				assert.Equal(t, 0, len(dls), "number of dependency links should be 0")
			},
		},
		{
			name:     "Test GetServices",
			endTs:    time.Date(2021, 7, 2, 1, 1, 1, 1, time.UTC),
			lookback: fourteenDays,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				collectionName := createNewCollectionName(uniqueCollectionName)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing-test").Collection(collectionName), nil)
				generateTraces(ctx, writer, 50, "circular", false)
				ops, err := reader.GetServices(ctx)
				if err != nil {
					t.Error(err)
				}
				fmt.Printf("Length of ops: %d\n", len(ops))
				assert.Equal(t, 50, len(ops), "number of services should be 50")
				for _, s := range ops {
					fmt.Printf("Service %s \n", s)
					arr := strings.Split(s, " ")
					name, num := arr[0], arr[1]
					numInt, err := strconv.Atoi(num)
					if err != nil {
						t.Error(err)
					}
					assert.Equal(t, "Service", name)
					assert.True(t, 0 <= numInt && numInt < 50)
				}
			},
		},
		{
			name:     "Test GetOperations",
			endTs:    time.Date(2021, 7, 2, 1, 1, 1, 1, time.UTC),
			lookback: fourteenDays,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				collectionName := createNewCollectionName(uniqueCollectionName)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing-test").Collection(collectionName), nil)
				generateTraces(ctx, writer, 50, "circular", false)
				ops, err := reader.GetOperations(ctx, spanstore.OperationQueryParameters{})
				if err != nil {
					t.Error(err)
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
			endTs:    time.Date(2021, 7, 2, 1, 1, 1, 1, time.UTC),
			lookback: fourteenDays,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				collectionName := createNewCollectionName(uniqueCollectionName)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing-test").Collection(collectionName), nil)
				generateTraces(ctx, writer, 50, "single", false)
				for i := 0; i < 50; i++ {
					trace, err := reader.GetTrace(ctx, model.TraceID{High: uint64(i), Low: uint64(i)})
					if err != nil {
						t.Error(err)
					}
					assert.Equal(t, fmt.Sprintf("Service %d", i), trace.GetSpans()[0].Process.ServiceName)
				}
			},
		},
		{
			name:     "Test Find Traces",
			endTs:    time.Date(2021, 7, 2, 1, 1, 1, 1, time.UTC),
			lookback: fourteenDays,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				collectionName := createNewCollectionName(uniqueCollectionName)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing-test").Collection(collectionName), nil)
				generateTraces(ctx, writer, 50, "single", false)
				traces, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
					StartTimeMin: time.Date(1997, 04, 30, 05, 1, 1, 1, time.UTC),
					StartTimeMax: time.Now(),
					NumTraces:    1500,
				})
				if err != nil {
					t.Error(err)
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
			endTs:    time.Date(2021, 7, 2, 1, 1, 1, 1, time.UTC),
			lookback: fourteenDays,
			runAssertion: func(endTs time.Time, lookback time.Duration) {
				collectionName := createNewCollectionName(uniqueCollectionName)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing-test").Collection(collectionName), nil)
				generateTraces(ctx, writer, 50, "circular", false)
				traces200, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
					StartTimeMin: time.Date(1997, 04, 30, 05, 1, 1, 1, time.UTC),
					StartTimeMax: time.Now(),
					NumTraces:    1500,
					Tags: map[string]string{
						"http.status_code": "200",
					},
				})
				if err != nil {
					t.Error(err)
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
					t.Error(err)
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
	// Clean up Database
	m.Database("jaeger-tracing-test").Drop(ctx)
}

// Unit tests

func TestReaderUnit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testCases := []struct {
		name         string
		runAssertion func()
	}{
		{
			name: "Test GetServices",
			runAssertion: func() {
				m := mock.NewMockReaderStorage(ctrl)
				m.
					EXPECT().
					Distinct(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(
						[]interface{}{
							"Service 1", "Service 2",
						},
						nil,
					)
				s := jaeger_mongodb.NewSpanReader(nil, m)
				svcs, err := s.GetServices(context.Background())
				if err != nil {
					t.Error(err)
				}
				for _, svc := range svcs {
					assert.Contains(t, [2]string{"Service 1", "Service 2"}, svc)
				}
			},
		},
		{
			name: "Test Get Operations",
			runAssertion: func() {
				m := mock.NewMockReaderStorage(ctrl)
				m.
					EXPECT().
					Distinct(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(
						[]interface{}{
							"Operation 1", "Operation 2",
						},
						nil,
					)
				s := jaeger_mongodb.NewSpanReader(nil, m)
				ops, err := s.GetOperations(context.Background(), spanstore.OperationQueryParameters{})
				if err != nil {
					t.Error(err)
				}
				for _, op := range ops {
					fmt.Println(op)
					assert.Equal(t, "TODO", op.SpanKind)
					assert.Contains(t, [2]string{"Operation 1", "Operation 2"}, op.Name)
				}
			},
		},
		// TODO: Add more unit tests.
	}
	for _, tc := range testCases {
		tc.runAssertion()
	}

}
func BenchmarkTagFiltering(b *testing.B) {
	mongoURL := os.Getenv("MONGO_URL")
	if mongoURL == "" {
		b.Skip("set MONGO_URL to run the benchmark tests")
	}
	m, err := mongo.Connect(context.TODO(), options.Client().
		ApplyURI(mongoURL).
		SetWriteConcern(writeconcern.New(writeconcern.W(1))))
	if err != nil {
		b.Error(err)
	}
	uniqueCollectionName := map[string]int{}

	ctx := context.TODO()

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
				collectionName := createNewCollectionName(uniqueCollectionName)
				writer := jaeger_mongodb.NewSpanWriter(m.Database("jaeger-tracing-test").Collection(collectionName), nil)
				readerStorage := jaeger_mongodb.NewMongoReaderStorage(m.Database("jaeger-tracing-test").Collection(collectionName))
				reader := jaeger_mongodb.NewSpanReader(nil, readerStorage)
				generateTraces(ctx, writer, 100, "circular", false)
				_, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
					StartTimeMin: time.Date(2021, 7, 1, 1, 1, 1, 1, time.UTC),
					StartTimeMax: time.Date(2021, 7, 3, 1, 1, 1, 1, time.UTC),
					Tags:         tags_in,
					NumTraces:    100,
				})
				if err != nil {
					b.Error(err)
				}
			},
		},
	}
	for _, tc := range testCases {
		tc.runAssertion(tc.tags)
	}
	// Clean up Database
	m.Database("jaeger-tracing-test").Drop(ctx)
}
