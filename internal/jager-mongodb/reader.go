package jager_mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	ErrTraceNotFound = errors.New("trace not found")
)

// SpanReader queries for traces in MongoDB.
type SpanReader struct {
	collection *mongo.Collection
	log        hclog.Logger
}

func NewSpanReader(collection *mongo.Collection, logger hclog.Logger) *SpanReader {
	return &SpanReader{
		collection: collection,
		log:        logger,
	}
}

// GetTrace retrieve the given traceID.
func (s *SpanReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	tracesMap, err := s.findTraces(ctx, []string{traceID.String()})
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
	opts := options.Distinct().SetMaxTime(2 * time.Second)

	services, err := s.collection.Distinct(ctx, "process.serviceName", bson.D{}, opts)

	if err != nil {
		return nil, fmt.Errorf("distinct call failed: %s", err)
	}

	return toStringArray(services)
}

// GetOperations returns all operation names for a given service
// known to the backend from spans within its retention period.
func (s *SpanReader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	opts := options.Distinct().SetMaxTime(2 * time.Second)

	filter := bson.D{}
	if query.ServiceName != "" {
		filter = bson.D{{"process.serviceName", query.ServiceName}}
	}

	ops, err := s.collection.Distinct(ctx, "operationName", filter, opts)

	if err != nil {
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
	ids, err := s.findTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}

	tracesMap, err := s.findTraces(ctx, ids)
	if err != nil {
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
	return nil, nil
}

// Internal method used to find traces
func (s *SpanReader) findTraces(ctx context.Context, ids []string) (map[string]*model.Trace, error) {
	filter := bson.M{
		"traceID": bson.M{"$in": ids},
	}

	findOpts := options.FindOptions{}
	cur, err := s.collection.Find(ctx, filter, &findOpts)

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

		pTags, err := s.convertKeyValues(ms.Process.Tags)
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
			Logs:          []model.Log{}, // TODO(dmichel): implement
			Process: &model.Process{
				ServiceName: ms.Process.ServiceName,
				Tags:        pTags,
			},
			Warnings: []string{}, // TODO(dmichel): implement
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
	filter := bson.M{}

	filter["startTime"] = bson.M{
		"$gt": query.StartTimeMin,
		"$lt": query.StartTimeMax,
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

	opts := options.DistinctOptions{}
	traceIds, err := s.collection.Distinct(ctx, "traceID", filter, &opts)
	if err != nil {
		s.log.Error("error getting traceIDs", "err", err)
		return nil, fmt.Errorf("error getting traceIDs: %w", err)
	}

	ids, err := toStringArray(traceIds)
	if err != nil {
		s.log.Error("error transforming traceIDs", "err", err)
		return nil, fmt.Errorf("error transforming traceIDs: %w", err)
	}

	limit := query.NumTraces
	if limit > len(ids) {
		limit = len(ids)
	}

	return ids[:limit], nil
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
