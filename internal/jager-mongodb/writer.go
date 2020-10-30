package jager_mongodb

import (
	"context"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type SpanWriter struct {
	collection *mongo.Collection
	log        hclog.Logger
}

func NewSpanWriter(collection *mongo.Collection, logger hclog.Logger) *SpanWriter {
	return &SpanWriter{
		collection: collection,
		log:        logger,
	}
}

// Write a span into MongoDB.
func (s *SpanWriter) WriteSpan(ctx context.Context, span *model.Span) error {
	mSpan := Span{
		TraceID:       span.TraceID.String(),
		SpanID:        span.SpanID.String(),
		OperationName: span.OperationName,
		StartTime:     span.StartTime,
		Duration:      span.Duration.Microseconds(),
		References:    convertReferences(span),
		ProcessID:     span.ProcessID,
		Process:       convertProcess(span.Process),
		Tags:          convertKeyValues(span.Tags),
		//Logs:          nil,
		Warnings: span.Warnings,
	}
	b, err := bson.Marshal(mSpan)

	if err != nil {
		return err
	}
	_, err = s.collection.InsertOne(ctx, b)
	return err
}

func convertProcess(process *model.Process) Process {
	return Process{
		ServiceName: process.ServiceName,
		Tags:        convertKeyValues(process.Tags),
	}
}

func convertReferences(span *model.Span) []Reference {
	out := make([]Reference, 0, len(span.References))
	for _, ref := range span.References {
		out = append(out, Reference{
			RefType: convertRefType(ref.RefType),
			TraceID: TraceID(ref.TraceID.String()),
			SpanID:  SpanID(ref.SpanID.String()),
		})
	}
	return out
}

func convertRefType(refType model.SpanRefType) ReferenceType {
	if refType == model.FollowsFrom {
		return FollowsFrom
	}
	return ChildOf
}

func convertKeyValues(keyValues model.KeyValues) []KeyValue {
	kvs := make([]KeyValue, 0)
	for _, kv := range keyValues {
		if kv.GetVType() != model.BinaryType {
			kvs = append(kvs, convertKeyValue(kv))
		}
	}
	return kvs
}

func convertKeyValue(kv model.KeyValue) KeyValue {
	return KeyValue{
		Key:   kv.Key,
		Type:  ValueType(strings.ToLower(kv.VType.String())),
		Value: kv.AsString(),
	}
}
