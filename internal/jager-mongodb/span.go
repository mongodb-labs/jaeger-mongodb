package jager_mongodb

import "time"

// ReferenceType is the reference type of one span to another
type ReferenceType string

// TraceID is the shared trace ID of all spans in the trace.
type TraceID string

// SpanID is the id of a span
type SpanID string

// ValueType is the type of a value stored in KeyValue struct.
type ValueType string

const (
	// ChildOf means a span is the child of another span
	ChildOf ReferenceType = "CHILD_OF"
	// FollowsFrom means a span follows from another span
	FollowsFrom ReferenceType = "FOLLOWS_FROM"

	// StringType indicates a string value stored in KeyValue
	StringType ValueType = "string"
	// BoolType indicates a Boolean value stored in KeyValue
	BoolType ValueType = "bool"
	// Int64Type indicates a 64bit signed integer value stored in KeyValue
	Int64Type ValueType = "int64"
	// Float64Type indicates a 64bit float value stored in KeyValue
	Float64Type ValueType = "float64"
)

// Span is MongoDB representation of the domain span.
type Span struct {
	TraceID       string      `bson:"traceID"`
	SpanID        string      `bson:"spanID"`
	OperationName string      `bson:"operationName"`
	StartTime     time.Time   `bson:"startTime"` // microseconds since Unix epoch
	Duration      int64       `bson:"duration"`  // microseconds
	References    []Reference `bson:"references"`
	ProcessID     string      `bson:"processID"`
	Process       Process     `bson:"process,omitempty"`
	Tags          []KeyValue  `bson:"tags"`
	Logs          []Log       `bson:"logs"`
	Warnings      []string    `bson:"warnings"`
}

// Reference is a reference from one span to another
type Reference struct {
	RefType ReferenceType `bson:"refType"`
	TraceID TraceID       `bson:"traceID"`
	SpanID  SpanID        `bson:"spanID"`
}

// Process is the process emitting a set of spans
type Process struct {
	ServiceName string     `bson:"serviceName"`
	Tags        []KeyValue `bson:"tags"`
}

// Log is a log emitted in a span
type Log struct {
	Timestamp uint64     `bson:"timestamp"`
	Fields    []KeyValue `bson:"fields"`
}

// KeyValue is a key-value pair with typed value.
type KeyValue struct {
	Key   string      `bson:"key"`
	Type  ValueType   `bson:"type,omitempty"`
	Value interface{} `bson:"value"`
}
