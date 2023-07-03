package jaeger_mongodb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

var (
	ErrTraceNotFound         = errors.New("trace not found")
	Empty                    struct{}
	maxTracesForDependencies = 25000
	tracer                   = otel.Tracer("reader")
)

type ReaderStorage interface {
	Distinct(ctx context.Context, field string, filter interface{}, opts *options.DistinctOptions) ([]interface{}, error)
	Find(ctx context.Context, filter interface{}, opts *options.FindOptions) (*mongo.Cursor, error)
}

type MongoReaderStorage struct {
	c *mongo.Collection
}

func (m MongoReaderStorage) Distinct(ctx context.Context, field string, filter interface{}, opts *options.DistinctOptions) ([]interface{}, error) {
	return m.c.Distinct(ctx, field, filter, opts)
}

func (m MongoReaderStorage) Find(ctx context.Context, filter interface{}, opts *options.FindOptions) (*mongo.Cursor, error) {
	return m.c.Find(ctx, filter, opts)
}

func NewMongoReaderStorage(c *mongo.Collection) *MongoReaderStorage {
	return &MongoReaderStorage{c: c}
}

// SpanReader queries for traces in MongoDB.
type SpanReader struct {
	storage              ReaderStorage
	log                  hclog.Logger
	mongoTimeoutDuration time.Duration
}

func NewSpanReader(readerStorage ReaderStorage, logger hclog.Logger, mongoTimeoutDuration time.Duration) *SpanReader {
	return &SpanReader{
		log:                  logger,
		storage:              readerStorage,
		mongoTimeoutDuration: mongoTimeoutDuration,
	}
}

// GetTrace retrieve the given traceID.
func (s *SpanReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	ctx, span := tracer.Start(ctx, "GetTrace")
	defer span.End()

	span.SetAttributes(attribute.Key("traceID").String(traceID.String()))
	tracesMap, err := s.fetchTracesById(ctx, []string{traceID.String()})
	if err != nil {
		return nil, err
	}
	for i := range tracesMap {
		return tracesMap[i], nil
	}
	return nil, ErrTraceNotFound
}

// GetServices returns all service names known to the backend from spans
// within its retention period.
func (s *SpanReader) GetServices(ctx context.Context) ([]string, error) {
	_, span := tracer.Start(ctx, "GetServices")
	defer span.End()

	opts := options.Distinct().SetMaxTime(s.mongoTimeoutDuration)

	services, err := s.storage.Distinct(ctx, "process.serviceName", bson.D{}, opts)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("distinct call failed: %s", err)
	}

	return toStringArray(services)
}

// GetOperations returns all operation names for a given service
// known to the backend from spans within its retention period.
func (s *SpanReader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	_, span := tracer.Start(ctx, "GetOperations")
	defer span.End()

	opts := options.Distinct().SetMaxTime(s.mongoTimeoutDuration)
	filter := bson.D{}
	if query.ServiceName != "" {
		filter = bson.D{{Key: "process.serviceName", Value: query.ServiceName}}
		span.SetAttributes(attribute.Key("process.serviceName").String(query.ServiceName))
	}
	ops, err := s.storage.Distinct(ctx, "operationName", filter, opts)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("distinct call failed: %s", err)
	}

	return toStringOperations(ops)
}

// FindTraces returns all traces matching query parameters. There's currently
// an implementation-dependent abiguity whether all query filters (such as
// multiple tags) must apply to the same span within a trace, or can be satisfied
// by different spans.
//
// If no matching traces are found, the function returns (nil, nil).
func (s *SpanReader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	ctx, span := tracer.Start(ctx, "FindTraces")
	defer span.End()

	ids, err := s.findTraceIDs(ctx, query)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}

	tracesMap, err := s.fetchTracesById(ctx, ids)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var traces []*model.Trace
	for _, trace := range tracesMap {
		traces = append(traces, trace)
	}

	return traces, nil
}

// FindTraceIDs does the same search as FindTraces, but returns only the list
// of matching trace IDs.
//
// If no matching traces are found, the function returns (nil, nil).
func (s *SpanReader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	_, span := tracer.Start(ctx, "FindTraceIDs")
	defer span.End()

	ids, err := s.findTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}

	traceIDs := make([]model.TraceID, len(ids))
	for i, id := range ids {
		t, err := model.TraceIDFromString(id)
		if err != nil {
			return nil, err
		}
		traceIDs[i] = t
	}

	return traceIDs, nil
}

func (s *SpanReader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	_, span := tracer.Start(ctx, "GetDependencies")
	defer span.End()

	traces, err := s.FindTraces(ctx,
		&spanstore.TraceQueryParameters{
			StartTimeMin: endTs.Add(-1 * lookback),
			StartTimeMax: endTs,
			NumTraces:    maxTracesForDependencies,
		},
	)
	if err != nil {
		zap.S().Error(err)
	}
	// Map spanID to serviceName.
	serviceNameBySpanID := make(map[model.SpanID]string)
	m := make(map[string]*model.DependencyLink)
	for _, trace := range traces {
		for _, s := range trace.Spans {
			serviceNameBySpanID[s.SpanID] = s.Process.ServiceName
		}
	}
	for _, trace := range traces {
		for _, s := range trace.Spans {
			for _, ref := range s.References {
				if ref.GetRefType() == model.SpanRefType_CHILD_OF && serviceNameBySpanID[ref.SpanID] != "" {
					parent, child := serviceNameBySpanID[ref.SpanID], s.Process.ServiceName
					dl := m[parent+child]
					if dl == nil {
						dl = &model.DependencyLink{
							Parent:    parent,
							Child:     child,
							CallCount: 0,
						}
						m[parent+child] = dl
					}
					dl.CallCount++
				}
			}
		}
	}

	dls := []model.DependencyLink{}
	for _, dl := range m {
		if dl.Parent != dl.Child {
			dls = append(dls, *dl)
		}
	}
	return dls, nil
}

// Internal method used to find traces
func (s *SpanReader) fetchTracesById(ctx context.Context, ids []string) (map[string]*model.Trace, error) {
	_, span := tracer.Start(ctx, "fetchTracesById")
	defer span.End()

	filter := bson.M{
		"traceID": bson.M{"$in": ids},
	}

	findOpts := options.FindOptions{}
	cur, err := s.storage.Find(ctx, filter, &findOpts)

	if err != nil {
		s.log.Error("error finding spans", "err", err)
		return nil, fmt.Errorf("error finding spans %w", err)
	}

	defer cur.Close(ctx)

	tracesMap := make(map[string]*model.Trace, len(ids))
	for cur.Next(ctx) {
		var ms Span
		err := cur.Decode(&ms)

		if err != nil {
			s.log.Error("error decoding span", "err", err)
			return nil, fmt.Errorf("error decoding span: %w", err)
		}

		if _, ok := tracesMap[ms.TraceID]; !ok {
			tracesMap[ms.TraceID] = &model.Trace{}
		}

		tId, err := model.TraceIDFromString(ms.TraceID)
		if err != nil {
			return nil, err
		}

		sId, err := model.SpanIDFromString(ms.SpanID)
		if err != nil {
			return nil, err
		}

		refs, err := s.convertRefs(ms.References)
		if err != nil {
			return nil, err
		}
		tags, err := s.convertKeyValues(ms.Tags)
		if err != nil {
			return nil, err
		}
		pTags, err := s.convertKeyValues(ms.Process.Tags)
		if err != nil {
			return nil, err
		}
		logs, err := s.convertLogs(ms.Logs)
		if err != nil {
			return nil, err
		}

		s := model.Span{
			TraceID:       tId,
			SpanID:        sId,
			OperationName: ms.OperationName,
			References:    refs,
			StartTime:     ms.StartTime,
			Duration:      model.MicrosecondsAsDuration(uint64(ms.Duration)),
			Tags:          tags,
			Logs:          logs,
			Process: &model.Process{
				ServiceName: ms.Process.ServiceName,
				Tags:        pTags,
			},
			Warnings: ms.Warnings,
		}

		tracesMap[ms.TraceID].Spans = append(tracesMap[ms.TraceID].Spans, &s)
	}

	if err := cur.Err(); err != nil {
		s.log.Error("error decoding span", "err", err)
		return nil, fmt.Errorf("error with finding span %w", err)
	}

	return tracesMap, nil
}

// Internal method used to find traceIDs.
func (s *SpanReader) findTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]string, error) {
	_, span := tracer.Start(ctx, "findTraceIds")
	defer span.End()

	filter := bson.M{}

	filter["startTime"] = bson.M{
		"$gt": query.StartTimeMin,
		"$lt": query.StartTimeMax,
	}

	// Filtering by concatenation of tags.
	tags_array := bson.A{}
	for k, v := range query.Tags {
		tags_array = append(tags_array, bson.M{"tags": bson.M{"$elemMatch": bson.M{"key": k, "value": v}}})
	}
	if len(tags_array) != 0 {
		filter["$and"] = tags_array
	}

	if query.ServiceName != "" {
		filter["process.serviceName"] = query.ServiceName
	}

	if query.OperationName != "" {
		filter["operationName"] = query.OperationName
	}

	if query.DurationMax != 0 {
		filter["duration"] = bson.M{
			"$lte": query.DurationMax.Microseconds(),
		}
	}

	if query.DurationMin != 0 {
		filter["duration"] = bson.M{
			"$gte": query.DurationMin.Microseconds(),
		}
	}

	if query.DurationMax != 0 && query.DurationMin != 0 {
		filter["duration"] = bson.M{
			"$lte": query.DurationMax.Microseconds(),
			"$gte": query.DurationMin.Microseconds(),
		}
	}

	opts := options.FindOptions{
		Projection: bson.D{{"traceID", 1}, {"_id", 0}},
		Sort:       bson.D{{"startTime", -1}},
	}

	cursor, err := s.storage.Find(ctx, filter, &opts)
	defer cursor.Close(ctx)

	if err != nil {
		s.log.Error("error getting traceIDs", "err", err)
		return nil, fmt.Errorf("error getting traceIDs: %w", err)
	}

	traceIds := make(map[string]interface{})
	for cursor.Next(ctx) {
		var span Span
		if err = cursor.Decode(&span); err != nil {
			log.Fatal(err)
		}
		traceIds[span.TraceID] = Empty

		// Only fetch as many tracesIds up to the limit.
		if len(traceIds) >= query.NumTraces {
			break
		}
	}

	ids := make([]string, 0, query.NumTraces)
	for k, _ := range traceIds {
		ids = append(ids, k)
	}

	return ids, nil
}

func toStringArray(arr []interface{}) ([]string, error) {
	array := make([]string, len(arr))

	for i, s := range arr {
		str, ok := s.(string)
		if !ok {
			return nil, errors.New("non-string key found")
		}
		array[i] = str
	}

	return array, nil
}

func toStringOperations(arr []interface{}) ([]spanstore.Operation, error) {
	array := make([]spanstore.Operation, len(arr))

	for i, s := range arr {
		str, ok := s.(string)
		if !ok {
			return nil, errors.New("non-string key found")
		}
		array[i] = spanstore.Operation{
			Name:     str,
			SpanKind: "TODO",
		}
	}

	return array, nil
}

func (s *SpanReader) convertRefs(refs []Reference) ([]model.SpanRef, error) {
	retMe := make([]model.SpanRef, len(refs))
	for i, r := range refs {
		// There are some inconsistencies with ReferenceTypes, hence the hacky fix.
		var refType model.SpanRefType
		switch r.RefType {
		case ChildOf:
			refType = model.ChildOf
		case FollowsFrom:
			refType = model.FollowsFrom
		default:
			return nil, fmt.Errorf("not a valid SpanRefType string %s", string(r.RefType))
		}

		traceID, err := model.TraceIDFromString(string(r.TraceID))
		if err != nil {
			return nil, err
		}

		spanID, err := model.SpanIDFromString(string(r.SpanID))
		if err != nil {
			return nil, err
		}

		retMe[i] = model.SpanRef{
			RefType: refType,
			TraceID: traceID,
			SpanID:  spanID,
		}
	}
	return retMe, nil
}

func (s *SpanReader) convertKeyValues(tags []KeyValue) ([]model.KeyValue, error) {
	retMe := make([]model.KeyValue, len(tags))
	for i := range tags {
		kv, err := s.convertKeyValue(&tags[i])
		if err != nil {
			return nil, err
		}
		retMe[i] = kv
	}
	return retMe, nil
}

func (s *SpanReader) convertKeyValue(tag *KeyValue) (model.KeyValue, error) {
	if tag.Value == nil {
		return model.KeyValue{}, fmt.Errorf("invalid nil Value in %v", tag)
	}
	tagValue, ok := tag.Value.(string)
	if !ok {
		return model.KeyValue{}, fmt.Errorf("non-string Value of type %t in %v", tag.Value, tag)
	}
	switch tag.Type {
	case StringType:
		return model.String(tag.Key, tagValue), nil
	case BoolType:
		value, err := strconv.ParseBool(tagValue)
		if err != nil {
			return model.KeyValue{}, err
		}
		return model.Bool(tag.Key, value), nil
	case Int64Type:
		value, err := strconv.ParseInt(tagValue, 10, 64)
		if err != nil {
			return model.KeyValue{}, err
		}
		return model.Int64(tag.Key, value), nil
	case Float64Type:
		value, err := strconv.ParseFloat(tagValue, 64)
		if err != nil {
			return model.KeyValue{}, err
		}
		return model.Float64(tag.Key, value), nil
	}
	return model.KeyValue{}, fmt.Errorf("not a valid ValueType string %s", string(tag.Type))
}

func (s *SpanReader) convertLogs(logs []Log) ([]model.Log, error) {
	convertedLogs := make([]model.Log, len(logs))
	for i, log := range logs {
		fields, err := s.convertKeyValues(log.Fields)
		if err != nil {
			return []model.Log{}, err
		}
		convertedLog := model.Log{
			Timestamp: time.UnixMilli(int64(log.Timestamp)),
			Fields:    fields,
		}
		convertedLogs[i] = convertedLog
	}
	return convertedLogs, nil
}
