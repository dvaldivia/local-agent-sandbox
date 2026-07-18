// Copyright 2026 Daniel Valdivia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func newTestRuntime(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	root := t.TempDir()
	srv := httptest.NewServer(NewServer(root))
	t.Cleanup(srv.Close)
	return srv, root
}

func TestHealth(t *testing.T) {
	srv, _ := newTestRuntime(t)
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Fatalf("health = %v", body)
	}
}

func TestExecute(t *testing.T) {
	srv, _ := newTestRuntime(t)
	post := func(cmd string) executeResponse {
		payload, _ := json.Marshal(executeRequest{Command: cmd})
		resp, err := http.Post(srv.URL+"/execute", "application/json", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("execute status %d", resp.StatusCode)
		}
		var out executeResponse
		json.NewDecoder(resp.Body).Decode(&out)
		return out
	}

	if r := post("echo hello"); strings.TrimSpace(r.Stdout) != "hello" || r.ExitCode != 0 {
		t.Errorf("echo: %+v", r)
	}
	// Non-zero exit propagates as exit_code with HTTP 200.
	if r := post("sh -c 'exit 3'"); r.ExitCode != 3 {
		t.Errorf("exit 3: got exit_code %d", r.ExitCode)
	}
	// Missing binary => exit_code 1, HTTP 200 (no shell).
	if r := post("this-binary-does-not-exist-xyz"); r.ExitCode != 1 {
		t.Errorf("missing binary: %+v", r)
	}
	// No shell interpretation: a pipe is passed as literal args to echo.
	if r := post("echo a | wc -l"); !strings.Contains(r.Stdout, "| wc -l") {
		t.Errorf("expected no shell interpretation, got %q", r.Stdout)
	}
}

func TestUploadDownloadRoundTrip(t *testing.T) {
	srv, _ := newTestRuntime(t)
	content := []byte("hello, world!\n")

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	part, _ := mw.CreateFormFile("file", "greeting.txt")
	part.Write(content)
	mw.Close()

	resp, err := http.Post(srv.URL+"/upload", mw.FormDataContentType(), &buf)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("upload status %d", resp.StatusCode)
	}

	// download.
	dl, err := http.Get(srv.URL + "/download/greeting.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Body.Close()
	got, _ := io.ReadAll(dl.Body)
	if !bytes.Equal(got, content) {
		t.Fatalf("download mismatch: %q", got)
	}

	// exists true / false.
	if !existsCall(t, srv.URL, "greeting.txt") {
		t.Error("greeting.txt should exist")
	}
	if existsCall(t, srv.URL, "nope.txt") {
		t.Error("nope.txt should not exist")
	}

	// download missing => 404.
	miss, _ := http.Get(srv.URL + "/download/nope.txt")
	if miss.StatusCode != 404 {
		t.Errorf("missing download status = %d", miss.StatusCode)
	}
	miss.Body.Close()
}

func TestList(t *testing.T) {
	srv, root := newTestRuntime(t)
	writeFile(t, root+"/a.txt", "aaa")
	writeFile(t, root+"/b.txt", "bb")

	resp, err := http.Get(srv.URL + "/list/.")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var entries []fileEntry
	json.NewDecoder(resp.Body).Decode(&entries)
	if len(entries) != 2 {
		t.Fatalf("list = %d entries, want 2: %+v", len(entries), entries)
	}
	// list of a file => 404.
	r2, _ := http.Get(srv.URL + "/list/a.txt")
	if r2.StatusCode != 404 {
		t.Errorf("list of file status = %d", r2.StatusCode)
	}
	r2.Body.Close()
}

// TestSafePathGuard unit-tests the containment guard independent of routing.
func TestSafePathGuard(t *testing.T) {
	root := t.TempDir()
	s := NewServer(root)
	for _, p := range []string{"../etc/passwd", "../../secret", "a/../../b", "/../../etc/passwd"} {
		if _, err := s.safePath(p); err == nil {
			t.Errorf("safePath(%q) should be rejected", p)
		}
	}
	for _, p := range []string{"file.txt", "sub/file.txt", "/abs.txt", "."} {
		if _, err := s.safePath(p); err != nil {
			t.Errorf("safePath(%q) should be allowed: %v", p, err)
		}
	}
}

// TestPathTraversalRejectedHTTP ensures the SDK's percent-encoded ".." escapes
// (the only form the SDK actually sends) are refused, not served.
func TestPathTraversalRejectedHTTP(t *testing.T) {
	srv, _ := newTestRuntime(t)
	// The SDK percent-encodes every special byte: ".." -> %2E%2E, "/" -> %2F.
	for _, p := range []string{"%2E%2E%2Fetc%2Fpasswd", "%2E%2E%2F%2E%2E%2Fsecret"} {
		resp, err := http.Get(srv.URL + "/download/" + p)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Errorf("encoded traversal %q was served (status 200), want rejection", p)
		}
	}
}

// TestSubdirEncodedPath verifies a percent-encoded subdirectory path (a%2Fb)
// round-trips — the SDK encodes "/" as %2F within a single path segment.
func TestSubdirEncodedPath(t *testing.T) {
	srv, root := newTestRuntime(t)
	if err := os.MkdirAll(root+"/logs", 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root+"/logs/app.log", "line1\n")

	resp, err := http.Get(srv.URL + "/download/logs%2Fapp.log")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "line1\n" {
		t.Fatalf("encoded subdir download: status=%d body=%q", resp.StatusCode, body)
	}
}

func existsCall(t *testing.T, base, path string) bool {
	t.Helper()
	resp, err := http.Get(base + "/exists/" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	b, _ := out["exists"].(bool)
	return b
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
