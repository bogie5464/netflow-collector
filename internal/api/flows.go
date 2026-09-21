package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
	// timeFormat is RFC 3339 UTC with fixed microsecond precision, matching
	// what both storage engines keep.
	timeFormat = "2006-01-02T15:04:05.000000Z07:00"
)

// FlowsRequest is the only description of GET /v1/flows' parameters. The
// binder, the validator and the OpenAPI builder all read these tags.
type FlowsRequest struct {
	Start      time.Time  `query:"start" validate:"required" doc:"Start of the half-open range [start, end) on received_at, RFC 3339 UTC."`
	End        time.Time  `query:"end" validate:"required,gtfield=Start" doc:"End of the range, RFC 3339 UTC. Must be strictly after start."`
	ExporterID *int64     `query:"exporter_id" validate:"omitempty,min=1" doc:"Only records from this exporter id."`
	SrcAddr    netip.Addr `query:"src_addr" doc:"Exact-match source address, IPv4 or IPv6."`
	DstAddr    netip.Addr `query:"dst_addr" doc:"Exact-match destination address, IPv4 or IPv6."`
	Protocol   *int       `query:"protocol" validate:"omitempty,min=0,max=255" doc:"IANA protocol number: 6 TCP, 17 UDP, 1 ICMP."`
	Limit      int        `query:"limit" doc:"Records per page. Defaults to 100 and is clamped to 1000; a value below 1 means the default."`
	Cursor     string     `query:"cursor" doc:"Opaque cursor from a previous response's meta.next_cursor."`
}

// ExportersRequest has no parameters; the fleet is bounded so it is unpaginated.
type ExportersRequest struct{}

// FlowJSON is one record as the API renders it.
type FlowJSON struct {
	ReceivedAt    string     `json:"received_at"`
	ExporterID    int64      `json:"exporter_id"`
	ExporterAddr  netip.Addr `json:"exporter_addr"`
	FlowType      string     `json:"flow_type"`
	FirstSwitched string     `json:"first_switched"`
	LastSwitched  string     `json:"last_switched"`
	SrcAddr       netip.Addr `json:"src_addr"`
	DstAddr       netip.Addr `json:"dst_addr"`
	SrcPort       uint16     `json:"src_port"`
	DstPort       uint16     `json:"dst_port"`
	Protocol      uint8      `json:"protocol"`
	TCPFlags      uint8      `json:"tcp_flags"`
	Packets       uint64     `json:"packets"`
	Bytes         uint64     `json:"bytes"`
	SamplingRate  uint32     `json:"sampling_rate"`
	InputIface    uint32     `json:"input_iface"`
	OutputIface   uint32     `json:"output_iface"`
	SrcAS         uint32     `json:"src_as"`
	DstAS         uint32     `json:"dst_as"`
	NextHop       *string    `json:"next_hop"`
}

func toJSON(r flow.FlowRecord) FlowJSON {
	out := FlowJSON{
		ReceivedAt: r.ReceivedAt.UTC().Format(timeFormat), ExporterID: r.ExporterID, ExporterAddr: r.ExporterAddr,
		FlowType: string(r.FlowType), FirstSwitched: r.FirstSwitched.UTC().Format(timeFormat), LastSwitched: r.LastSwitched.UTC().Format(timeFormat),
		SrcAddr: r.SrcAddr, DstAddr: r.DstAddr, SrcPort: r.SrcPort, DstPort: r.DstPort, Protocol: r.Protocol, TCPFlags: r.TCPFlags,
		Packets: r.Packets, Bytes: r.Bytes, SamplingRate: r.SamplingRate, InputIface: r.InputIface, OutputIface: r.OutputIface,
		SrcAS: r.SrcAS, DstAS: r.DstAS,
	}
	if r.NextHop.IsValid() {
		nh := r.NextHop.String()
		out.NextHop = &nh
	}
	return out
}

// ExporterJSON is one exporter as the API renders it.
type ExporterJSON struct {
	ID          int64      `json:"id"`
	IPAddress   netip.Addr `json:"ip_address"`
	Label       *string    `json:"label"`
	FirstSeenAt string     `json:"first_seen_at"`
	LastSeenAt  string     `json:"last_seen_at"`
}

func toExporterJSON(e flow.Exporter) ExporterJSON {
	return ExporterJSON{
		ID: e.ID, IPAddress: e.IPAddress, Label: e.Label,
		FirstSeenAt: e.FirstSeenAt.UTC().Format(timeFormat),
		LastSeenAt:  e.LastSeenAt.UTC().Format(timeFormat),
	}
}

// route ties a method and path to a request struct and a handler; the
// OpenAPI builder walks the same table.
type route struct {
	Method, Path, Summary string
	Request               reflect.Type
	handler               func(*Server, http.ResponseWriter, *http.Request)
}

var routes = []route{
	{"GET", "/v1/flows", "Cursor-paginated flow records over a bounded time range, with optional filters.", reflect.TypeFor[FlowsRequest](), (*Server).flows},
	{"GET", "/v1/exporters", "Every known exporter with first/last seen and label.", reflect.TypeFor[ExportersRequest](), (*Server).exporters},
}

func (s *Server) flows(w http.ResponseWriter, r *http.Request) {
	var req FlowsRequest
	if details := bind(r, &req); len(details) > 0 {
		writeError(w, r, http.StatusUnprocessableEntity, CodeValidation, "invalid parameters", details)
		return
	}
	if req.Cursor != "" {
		if err := checkCursor(req.Cursor); err != nil {
			writeError(w, r, http.StatusBadRequest, CodeBadRequest, "undecodable cursor", nil)
			return
		}
	}
	limit := req.Limit
	if limit < 1 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	q := flow.FlowQuery{
		Start: req.Start, End: req.End,
		SrcAddr: req.SrcAddr, DstAddr: req.DstAddr, Limit: limit, Cursor: req.Cursor,
	}
	if req.ExporterID != nil {
		q.ExporterID = *req.ExporterID
	}
	if req.Protocol != nil {
		p := uint8(*req.Protocol)
		q.Protocol = &p
	}
	if s.querier == nil {
		writeError(w, r, http.StatusServiceUnavailable, CodeBackendUnavailable, "no queryable storage backend is configured on this instance", nil)
		return
	}
	page, err := s.querier.Query(r.Context(), q)
	if err != nil {
		s.log.Error("query failed", "request_id", RequestID(r.Context()), "err", err)
		writeError(w, r, http.StatusServiceUnavailable, CodeBackendUnavailable, "the storage backend did not answer", nil)
		return
	}
	data := make([]FlowJSON, 0, len(page.Records))
	for _, rec := range page.Records {
		data = append(data, toJSON(rec))
	}
	writeData(w, data, meta{NextCursor: page.NextCursor, HasMore: page.HasMore})
}

func (s *Server) exporters(w http.ResponseWriter, r *http.Request) {
	var req ExportersRequest
	if details := bind(r, &req); len(details) > 0 {
		writeError(w, r, http.StatusUnprocessableEntity, CodeValidation, "invalid parameters", details)
		return
	}
	names := make([]string, 0, len(s.sinks))
	for name := range s.sinks {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		lister, ok := s.sinks[name].(flow.ExporterLister)
		if !ok {
			continue
		}
		list, err := lister.ListExporters(r.Context())
		if err != nil {
			s.log.Error("list exporters failed", "request_id", RequestID(r.Context()), "backend", name, "err", err)
			continue
		}
		data := make([]ExporterJSON, 0, len(list))
		for _, e := range list {
			data = append(data, toExporterJSON(e))
		}
		writeData(w, data, meta{HasMore: false})
		return
	}
	writeError(w, r, http.StatusServiceUnavailable, CodeBackendUnavailable, "no configured backend can list exporters", nil)
}

// cursor mirrors the backends' opaque token so the API can reject an
// undecodable one at the boundary without importing a backend package.
type cursor struct {
	T int64 `json:"t"`
	S int64 `json:"s"`
}

func checkCursor(s string) error {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	var c cursor
	return json.Unmarshal(raw, &c)
}

// bind parses the query string into req by its `query` tags, rejects unknown
// parameters, and runs the validator. It returns one Detail per problem.
func bind(r *http.Request, req any) []Detail {
	var details []Detail
	v := reflect.ValueOf(req).Elem()
	t := v.Type()
	known := map[string]bool{}
	for i := range t.NumField() {
		f := t.Field(i)
		name := f.Tag.Get("query")
		if name == "" {
			continue
		}
		known[name] = true
		raw := r.URL.Query().Get(name)
		if raw == "" {
			continue
		}
		if err := setField(v.Field(i), raw); err != nil {
			details = append(details, Detail{Field: name, Message: err.Error()})
		}
	}
	keys := make([]string, 0, len(r.URL.Query()))
	for k := range r.URL.Query() {
		if !known[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		details = append(details, Detail{Field: k, Message: "unknown parameter"})
	}
	if len(details) > 0 {
		return details
	}
	if err := requestValidator.Struct(req); err != nil {
		var ves validator.ValidationErrors
		if errors.As(err, &ves) {
			for _, fe := range ves {
				details = append(details, Detail{Field: fe.Field(), Message: describe(fe)})
			}
		} else {
			details = append(details, Detail{Field: "", Message: err.Error()})
		}
	}
	return details
}

var requestValidator = func() *validator.Validate {
	v := validator.New(validator.WithRequiredStructEnabled())
	v.RegisterTagNameFunc(func(f reflect.StructField) string { return f.Tag.Get("query") })
	return v
}()

func describe(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return "is required"
	case "gtfield":
		return "must be after " + strings.ToLower(fe.Param())
	case "min":
		return "must be at least " + fe.Param()
	case "max":
		return "must be at most " + fe.Param()
	default:
		return "failed " + fe.Tag()
	}
}

func setField(f reflect.Value, raw string) error {
	switch f.Interface().(type) {
	case time.Time:
		t, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return errors.New("must be an RFC 3339 timestamp")
		}
		f.Set(reflect.ValueOf(t.UTC()))
	case netip.Addr:
		a, err := netip.ParseAddr(raw)
		if err != nil {
			return errors.New("must be an IPv4 or IPv6 address")
		}
		f.Set(reflect.ValueOf(a))
	case *int:
		n, err := strconv.Atoi(raw)
		if err != nil {
			return errors.New("must be an integer")
		}
		f.Set(reflect.ValueOf(&n))
	case *int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return errors.New("must be an integer")
		}
		f.Set(reflect.ValueOf(&n))
	case int, int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return errors.New("must be an integer")
		}
		f.SetInt(n)
	case string:
		f.SetString(raw)
	default:
		return fmt.Errorf("unsupported parameter type %s", f.Type())
	}
	return nil
}

// OpenAPI renders the document from the route table and the request
// structs, so it cannot drift from the handlers. Output is deterministic:
// docs/openapi.json is this function's output, committed, and diffed by the
// build gate.
func OpenAPI() []byte {
	paths := map[string]any{}
	for _, r := range routes {
		var params []map[string]any
		for i := range r.Request.NumField() {
			f := r.Request.Field(i)
			name := f.Tag.Get("query")
			if name == "" {
				continue
			}
			rules := f.Tag.Get("validate")
			p := map[string]any{
				"name": name, "in": "query",
				"required":    strings.Contains(rules, "required"),
				"description": f.Tag.Get("doc"),
				"schema":      schemaFor(f.Type, rules),
			}
			params = append(params, p)
		}
		if params == nil {
			params = []map[string]any{}
		}
		paths[r.Path] = map[string]any{strings.ToLower(r.Method): map[string]any{
			"summary":    r.Summary,
			"parameters": params,
			"security":   []map[string]any{{"ApiKeyAuth": []string{}}},
			"responses": map[string]any{
				"200": map[string]any{"description": "Success envelope", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/SuccessEnvelope"}}}},
				"400": errResponse("Malformed query string or undecodable cursor"),
				"401": errResponse("Missing or unknown API key"),
				"422": errResponse("Well-formed request, semantically invalid parameter"),
				"503": errResponse("The storage backend did not answer"),
			},
		}}
	}
	doc := map[string]any{
		"openapi": "3.1.0",
		"info":    map[string]any{"title": "NetFlow Collector API", "version": "v1"},
		"paths":   paths,
		"components": map[string]any{
			"securitySchemes": map[string]any{"ApiKeyAuth": map[string]any{"type": "apiKey", "in": "header", "name": "Authorization", "description": "Bearer <key> from NFC_API_KEYS"}},
			"schemas": map[string]any{
				"SuccessEnvelope": map[string]any{"type": "object", "required": []string{"data", "meta"}, "properties": map[string]any{
					"data": map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
					"meta": map[string]any{"type": "object", "required": []string{"has_more"}, "properties": map[string]any{
						"next_cursor": map[string]any{"type": "string"}, "has_more": map[string]any{"type": "boolean"}}},
				}},
				"ErrorEnvelope": map[string]any{"type": "object", "required": []string{"error"}, "properties": map[string]any{
					"error": map[string]any{"type": "object", "required": []string{"code", "message", "request_id"}, "properties": map[string]any{
						"code":       map[string]any{"type": "string", "enum": []string{CodeValidation, CodeBadRequest, CodeUnauthorized, CodeNotFound, CodeMethodNotAllowed, CodeBackendUnavailable, CodeInternal}},
						"message":    map[string]any{"type": "string"},
						"details":    map[string]any{"type": "array", "items": map[string]any{"type": "object", "properties": map[string]any{"field": map[string]any{"type": "string"}, "message": map[string]any{"type": "string"}}}},
						"request_id": map[string]any{"type": "string"},
					}},
				}},
			},
		},
	}
	out, _ := json.MarshalIndent(doc, "", "  ") // map keys are sorted: deterministic
	return append(out, '\n')
}

func errResponse(desc string) map[string]any {
	return map[string]any{"description": desc, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/ErrorEnvelope"}}}}
}

func schemaFor(t reflect.Type, rules string) map[string]any {
	s := map[string]any{}
	switch t {
	case reflect.TypeFor[time.Time]():
		s["type"], s["format"] = "string", "date-time"
	case reflect.TypeFor[netip.Addr]():
		s["type"], s["format"] = "string", "ip"
	case reflect.TypeFor[string]():
		s["type"] = "string"
	default:
		s["type"] = "integer"
	}
	for _, rule := range strings.Split(rules, ",") {
		if k, v, ok := strings.Cut(rule, "="); ok {
			if n, err := strconv.Atoi(v); err == nil {
				switch k {
				case "min":
					s["minimum"] = n
				case "max":
					s["maximum"] = n
				}
			}
		}
	}
	return s
}
