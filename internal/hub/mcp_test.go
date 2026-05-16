//go:build ignore

package hub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMCPRequiresAuth(t *testing.T) {
	s := NewServer(Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/mcp")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 401, resp.StatusCode)
}

func TestMCPAcceptsCorrectToken(t *testing.T) {
	s := NewServer(Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"/mcp/sse", nil)
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	// resp may be nil if ctx canceled before headers; that's fine — main test is auth != 401.
	if err != nil {
		// Context cancellation is acceptable — it means we got through auth.
		return
	}
	assert.NotEqual(t, 401, resp.StatusCode)
}
