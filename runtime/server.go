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

// Command sandbox-runtime is the in-container HTTP API that the agent-sandbox
// SDKs talk to (through the router). It is byte-compatible with the reference
// Python runtime (kubernetes-sigs/agent-sandbox examples/python-runtime-sandbox/main.py):
// same routes, status codes, and JSON shapes, rooted at a working directory
// (default /app).
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Server serves the runtime API rooted at a base directory.
type Server struct {
	root string
	mux  *http.ServeMux
}

// NewServer builds a runtime server rooted at root (created if missing).
func NewServer(root string) *Server {
	s := &Server{root: root}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.health)
	mux.HandleFunc("POST /execute", s.execute)
	mux.HandleFunc("POST /upload", s.upload)
	mux.HandleFunc("GET /download/{path...}", s.download)
	mux.HandleFunc("GET /list/{path...}", s.list)
	mux.HandleFunc("GET /exists/{path...}", s.exists)
	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// safePath resolves p against the root, rejecting escapes outside it (mirrors
// the reference get_safe_path). p is a relative path; leading slashes are
// stripped so absolute-looking inputs stay within root.
func (s *Server) safePath(p string) (string, error) {
	base, err := filepath.Abs(s.root)
	if err != nil {
		return "", err
	}
	clean := strings.TrimLeft(p, "/")
	full := filepath.Join(base, clean)
	// filepath.Join already cleans "..", but verify containment explicitly.
	if full != base && !strings.HasPrefix(full, base+string(os.PathSeparator)) {
		return "", errors.New("access denied: path escapes root")
	}
	return full, nil
}

type executeRequest struct {
	Command string `json:"command"`
}

type executeResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "message": "Sandbox Runtime is active."})
}

func (s *Server) execute(w http.ResponseWriter, r *http.Request) {
	var req executeRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusOK, executeResponse{Stderr: "Failed to decode request: " + err.Error(), ExitCode: 1})
		return
	}
	args, err := splitArgs(req.Command)
	if err != nil || len(args) == 0 {
		msg := "empty command"
		if err != nil {
			msg = err.Error()
		}
		writeJSON(w, http.StatusOK, executeResponse{Stderr: "Failed to execute command: " + msg, ExitCode: 1})
		return
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = s.root
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	resp := executeResponse{Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: 0}
	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			resp.ExitCode = exitErr.ExitCode()
		} else {
			// Spawn failure (e.g. binary not found): mirror the reference,
			// which returns exit_code 1 with an explanatory stderr.
			resp.Stderr = "Failed to execute command: " + runErr.Error()
			resp.ExitCode = 1
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) upload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "invalid multipart form: " + err.Error()})
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "missing file field"})
		return
	}
	defer file.Close()

	dest, err := s.safePath(header.Filename)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"message": "Access denied"})
		return
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": "File upload failed: " + err.Error()})
		return
	}
	out, err := os.Create(dest)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": "File upload failed: " + err.Error()})
		return
	}
	defer out.Close()
	if _, err := io.Copy(out, file); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": "File upload failed: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": fmt.Sprintf("File '%s' uploaded successfully.", header.Filename)})
}

func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	full, err := s.safePath(r.PathValue("path"))
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"message": "Access denied"})
		return
	}
	fi, err := os.Stat(full)
	if err != nil || fi.IsDir() {
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "File not found"})
		return
	}
	f, err := os.Open(full)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "File not found"})
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f)
}

type fileEntry struct {
	Name    string  `json:"name"`
	Size    int64   `json:"size"`
	Type    string  `json:"type"`
	ModTime float64 `json:"mod_time"`
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	full, err := s.safePath(r.PathValue("path"))
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"message": "Access denied"})
		return
	}
	fi, err := os.Stat(full)
	if err != nil || !fi.IsDir() {
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "Path is not a directory"})
		return
	}
	dirents, err := os.ReadDir(full)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": "List files failed: " + err.Error()})
		return
	}
	entries := make([]fileEntry, 0, len(dirents))
	for _, de := range dirents {
		info, ierr := de.Info()
		if ierr != nil {
			continue
		}
		typ := "file"
		if de.IsDir() {
			typ = "directory"
		}
		entries = append(entries, fileEntry{
			Name:    de.Name(),
			Size:    info.Size(),
			Type:    typ,
			ModTime: float64(info.ModTime().UnixNano()) / 1e9,
		})
	}
	writeJSON(w, http.StatusOK, entries)
}

func (s *Server) exists(w http.ResponseWriter, r *http.Request) {
	decoded := r.PathValue("path")
	full, err := s.safePath(decoded)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"message": "Access denied"})
		return
	}
	_, statErr := os.Stat(full)
	writeJSON(w, http.StatusOK, map[string]any{"path": decoded, "exists": statErr == nil})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
