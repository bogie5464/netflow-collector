package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
)

// fakeQuerier serves pages out of an in-memory slice, newest first, with the
// same cursor shape the real backends use. It records the last query.
type fakeQuerier struct {
	records []flow.FlowRecord
	last    flow.FlowQuery
	err     error
}

func (f *fakeQuerier) Query(_ context.Context, q flow.FlowQuery) (flow.FlowResultPage, error) {
	f.last = q
	if f.err != nil {
		return flow.FlowResultPage{}, f.err
	}
	limit := q.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	start := 0
	if q.Cursor != "" {
		var c cursor
		if err := decodeInto(q.Cursor, &c); err != nil {
			return flow.FlowResultPage{}, errors.New("fake: undecodable cursor")
		}
		start = int(c.S) + 1
	}
	var page flow.FlowResultPage
	for i := start; i < len(f.records) && len(page.Records) < limit; i++ {
		page.Records = append(page.Records, f.records[i])
	}
	if start+len(page.Records) < len(f.records) {
		page.HasMore = true
		page.NextCursor = encode(cursor{T: 0, S: int64(start + len(page.Records) - 1)})
	}
	return page, nil
}

func decodeInto(s string, c *cursor) error {
	if err := checkCursor(s); err != nil {
		return err
	}
	raw, _ := base64Decode(s)
	return json.Unmarshal(raw, c)
}

type fakeLister struct {
	flow.Backend
	exporters []flow.Exporter
	err       error
}

func (f *fakeLister) ListExporters(context.Context) ([]flow.Exporter, error) {
	return f.exporters, f.err
}

func records(n int) []flow.FlowRecord {
	base := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	out := make([]flow.FlowRecord, n)
	for i := range out {
		out[i] = flow.FlowRecord{
			ReceivedAt: base.Add(-time.Duration(i) * time.Second), ExporterID: 3, ExporterAddr: netip.MustParseAddr("198.51.100.7"),
			FlowType: flow.FlowTypeNetFlow5, FirstSwitched: base, LastSwitched: base,
			SrcAddr: netip.MustParseAddr("203.0.113.10"), DstAddr: netip.MustParseAddr("198.51.100.200"),
			SrcPort: uint16(40000 + i), DstPort: 443, Protocol: 6, TCPFlags: 24, Packets: 12, Bytes: 9000, SamplingRate: 1,
			NextHop: netip.MustParseAddr("198.51.100.1"),
		}
	}
	return out
}

type envelope struct {
	Data  json.RawMessage `json:"data"`
	Meta  *meta           `json:"meta"`
	Error *errorBody      `json:"error"`
}

func get(t *testing.T, h http.Handler, path string, params url.Values) (*httptest.ResponseRecorder, envelope) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil)
	req.Header.Set("Authorization", "Bearer dev-local-key") // E2-T2 installs the check; sending it now keeps this gate green after it lands
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	var env envelope
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &env), "body: %s", rr.Body.String())
	return rr, env
}

func validRange() url.Values {
	return url.Values{"start": {"2026-09-19T00:00:00Z"}, "end": {"2026-09-20T00:00:00Z"}}
}

func newHandler(q flow.Querier, backends map[string]flow.Sink) http.Handler {
	return New(q, backends, config.Config{APIKeys: []string{"dev-local-key"}})
}

func TestFlowsSuccessEnvelope(t *testing.T) {
	fq := &fakeQuerier{records: records(3)}
	rr, env := get(t, newHandler(fq, nil), "/v1/flows", validRange())
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	require.NotEmpty(t, rr.Header().Get("X-Request-Id"))
	require.Nil(t, env.Error)
	require.NotNil(t, env.Meta)
	require.False(t, env.Meta.HasMore)
	require.Empty(t, env.Meta.NextCursor)
	var data []FlowJSON
	require.NoError(t, json.Unmarshal(env.Data, &data))
	require.Len(t, data, 3)
	require.Equal(t, "2026-09-19T12:00:00.000000Z", data[0].ReceivedAt)
	require.Equal(t, int64(3), data[0].ExporterID)
	require.Equal(t, "netflow5", data[0].FlowType)
	require.NotNil(t, data[0].NextHop)
	require.Equal(t, "198.51.100.1", *data[0].NextHop)
	require.True(t, fq.last.Start.Before(fq.last.End))
	require.Equal(t, defaultLimit, fq.last.Limit, "limit defaults to 100")
}

func TestFlowsValidation(t *testing.T) {
	fq := &fakeQuerier{records: records(3)}
	h := newHandler(fq, nil)
	cases := []struct {
		name   string
		params url.Values
		field  string
		msg    string
	}{
		{"missing start", url.Values{"end": {"2026-09-20T00:00:00Z"}}, "start", "is required"},
		{"missing end", url.Values{"start": {"2026-09-19T00:00:00Z"}}, "end", "is required"},
		{"end equal to start", url.Values{"start": {"2026-09-19T00:00:00Z"}, "end": {"2026-09-19T00:00:00Z"}}, "end", "must be after start"},
		{"end before start", url.Values{"start": {"2026-09-20T00:00:00Z"}, "end": {"2026-09-19T00:00:00Z"}}, "end", "must be after start"},
		{"unparseable start", url.Values{"start": {"yesterday"}, "end": {"2026-09-20T00:00:00Z"}}, "start", "RFC 3339"},
		{"exporter_id below 1", with(validRange(), "exporter_id", "0"), "exporter_id", "at least 1"},
		{"bad src_addr", with(validRange(), "src_addr", "300.1.1.1"), "src_addr", "address"},
		{"protocol above 255", with(validRange(), "protocol", "256"), "protocol", "at most 255"},
		{"unknown parameter", with(validRange(), "srcaddr", "1.2.3.4"), "srcaddr", "unknown parameter"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr, env := get(t, h, "/v1/flows", tc.params)
			require.Equal(t, http.StatusUnprocessableEntity, rr.Code, rr.Body.String())
			require.NotNil(t, env.Error)
			require.Equal(t, CodeValidation, env.Error.Code)
			require.Equal(t, rr.Header().Get("X-Request-Id"), env.Error.RequestID)
			var found bool
			for _, d := range env.Error.Details {
				if d.Field == tc.field {
					found = true
					require.Contains(t, d.Message, tc.msg)
				}
			}
			require.True(t, found, "details must name %q: %+v", tc.field, env.Error.Details)
		})
	}
}

func with(v url.Values, k, val string) url.Values {
	v.Set(k, val)
	return v
}

func TestFlowsLimitIsClamped(t *testing.T) {
	fq := &fakeQuerier{records: records(1200)}
	rr, env := get(t, newHandler(fq, nil), "/v1/flows", with(validRange(), "limit", "5000"))
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, maxLimit, fq.last.Limit)
	var data []FlowJSON
	require.NoError(t, json.Unmarshal(env.Data, &data))
	require.Len(t, data, maxLimit)
	require.True(t, env.Meta.HasMore)

	rr, _ = get(t, newHandler(fq, nil), "/v1/flows", with(validRange(), "limit", "-5"))
	require.Equal(t, http.StatusOK, rr.Code, "limit is never an error")
	require.Equal(t, defaultLimit, fq.last.Limit)
}

func TestFlowsCursorPagination(t *testing.T) {
	fq := &fakeQuerier{records: records(25)}
	h := newHandler(fq, nil)
	seen := map[uint16]bool{}
	params := with(validRange(), "limit", "10")
	pages := 0
	for {
		rr, env := get(t, h, "/v1/flows", params)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		var data []FlowJSON
		require.NoError(t, json.Unmarshal(env.Data, &data))
		for _, d := range data {
			require.False(t, seen[d.SrcPort], "record %d repeated", d.SrcPort)
			seen[d.SrcPort] = true
		}
		pages++
		if !env.Meta.HasMore {
			require.Empty(t, env.Meta.NextCursor)
			break
		}
		require.NotEmpty(t, env.Meta.NextCursor)
		params.Set("cursor", env.Meta.NextCursor)
	}
	require.Equal(t, 3, pages)
	require.Len(t, seen, 25)

	rr, env := get(t, h, "/v1/flows", with(validRange(), "cursor", "!!not-base64!!"))
	require.Equal(t, http.StatusBadRequest, rr.Code)
	require.Equal(t, CodeBadRequest, env.Error.Code)
	require.Equal(t, rr.Header().Get("X-Request-Id"), env.Error.RequestID)
}

func TestFlowsFiltersReachTheQuerier(t *testing.T) {
	fq := &fakeQuerier{}
	params := validRange()
	params.Set("exporter_id", "7")
	params.Set("src_addr", "2001:db8::1")
	params.Set("dst_addr", "10.0.0.2")
	params.Set("protocol", "17")
	rr, _ := get(t, newHandler(fq, nil), "/v1/flows", params)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Equal(t, int64(7), fq.last.ExporterID)
	require.Equal(t, netip.MustParseAddr("2001:db8::1"), fq.last.SrcAddr)
	require.Equal(t, netip.MustParseAddr("10.0.0.2"), fq.last.DstAddr)
	require.NotNil(t, fq.last.Protocol)
	require.Equal(t, uint8(17), *fq.last.Protocol)
}

func TestBackendErrorIsNeverLeaked(t *testing.T) {
	fq := &fakeQuerier{err: errors.New("postgres: connection reset by peer at 10.1.2.3")}
	rr, env := get(t, newHandler(fq, nil), "/v1/flows", validRange())
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Equal(t, CodeBackendUnavailable, env.Error.Code)
	require.NotContains(t, rr.Body.String(), "10.1.2.3")
	require.Equal(t, rr.Header().Get("X-Request-Id"), env.Error.RequestID)
}

func TestRoutingErrors(t *testing.T) {
	h := newHandler(&fakeQuerier{}, nil)
	rr, env := get(t, h, "/v1/nothing", nil)
	require.Equal(t, http.StatusNotFound, rr.Code)
	require.Equal(t, CodeNotFound, env.Error.Code)
	require.Equal(t, rr.Header().Get("X-Request-Id"), env.Error.RequestID)

	req := httptest.NewRequest(http.MethodPost, "/v1/flows", nil)
	req.Header.Set("Authorization", "Bearer dev-local-key")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &env))
	require.Equal(t, CodeMethodNotAllowed, env.Error.Code)
}

func TestPanicBecomesInternalError(t *testing.T) {
	h := newHandler(&panicQuerier{}, nil)
	rr, env := get(t, h, "/v1/flows", validRange())
	require.Equal(t, http.StatusInternalServerError, rr.Code)
	require.Equal(t, CodeInternal, env.Error.Code)
	require.Equal(t, rr.Header().Get("X-Request-Id"), env.Error.RequestID)
}

type panicQuerier struct{}

func (panicQuerier) Query(context.Context, flow.FlowQuery) (flow.FlowResultPage, error) {
	panic("boom")
}

func TestExporters(t *testing.T) {
	label := "core-rtr-1"
	lister := &fakeLister{exporters: []flow.Exporter{{ID: 3, IPAddress: netip.MustParseAddr("198.51.100.7"), Label: &label,
		FirstSeenAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), LastSeenAt: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)}}}
	rr, env := get(t, newHandler(&fakeQuerier{}, map[string]flow.Sink{"postgres": lister}), "/v1/exporters", nil)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.False(t, env.Meta.HasMore)
	var data []ExporterJSON
	require.NoError(t, json.Unmarshal(env.Data, &data))
	require.Len(t, data, 1)
	require.Equal(t, "core-rtr-1", *data[0].Label)

	rr, env = get(t, newHandler(&fakeQuerier{}, map[string]flow.Sink{"postgres": &fakeLister{}}), "/v1/exporters", url.Values{"limit": {"5"}})
	require.Equal(t, http.StatusUnprocessableEntity, rr.Code, "no parameters are accepted")
	require.Equal(t, "limit", env.Error.Details[0].Field)

	rr, env = get(t, newHandler(&fakeQuerier{}, map[string]flow.Sink{"postgres": nopBackend{}}), "/v1/exporters", nil)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code, "a backend without ListExporters is reported, not faked")
	require.Equal(t, CodeBackendUnavailable, env.Error.Code)
}

type nopBackend struct{ flow.Backend }

func TestOpenAPIIsDeterministicAndReflectsTheStruct(t *testing.T) {
	a, b := OpenAPI(), OpenAPI()
	require.Equal(t, a, b)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(a, &doc))
	paths := doc["paths"].(map[string]any)
	flows := paths["/v1/flows"].(map[string]any)["get"].(map[string]any)
	params := flows["parameters"].([]any)
	names := map[string]map[string]any{}
	for _, p := range params {
		pm := p.(map[string]any)
		names[pm["name"].(string)] = pm
	}
	for i := range reflectFields() {
		require.Contains(t, names, reflectFields()[i], "every query field is documented")
	}
	require.Equal(t, true, names["start"]["required"])
	require.Equal(t, false, names["limit"]["required"])
	require.Equal(t, float64(255), names["protocol"]["schema"].(map[string]any)["maximum"])
	require.Contains(t, paths, "/v1/exporters")
}

func reflectFields() []string {
	return []string{"start", "end", "exporter_id", "src_addr", "dst_addr", "protocol", "limit", "cursor"}
}

// The cursor codec the real backends use, so the fake pages like they do.
func encode(c cursor) string {
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func base64Decode(s string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(s) }
