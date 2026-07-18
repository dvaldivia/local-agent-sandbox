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

package driver

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/docker/docker/api/types"
)

// DefaultRuntimeImage is the tag used for the bundled runtime image.
const DefaultRuntimeImage = "lasd/sandbox-runtime:dev"

// BuildRuntimeImage builds the bundled runtime image from a build-context
// directory (the repo's runtime/ dir) and tags it. Requires only Docker — the
// runtime is stdlib-only so the multi-stage build is hermetic.
func (d *DockerDriver) BuildRuntimeImage(ctx context.Context, tag, contextDir string) error {
	tarball, err := tarDir(contextDir)
	if err != nil {
		return fmt.Errorf("driver: tar build context %q: %w", contextDir, err)
	}
	resp, err := d.cli.ImageBuild(ctx, bytes.NewReader(tarball), types.ImageBuildOptions{
		Tags:        []string{tag},
		Dockerfile:  "Dockerfile",
		Remove:      true,
		ForceRemove: true,
	})
	if err != nil {
		return fmt.Errorf("driver: image build: %w", err)
	}
	defer resp.Body.Close()
	// Drain the build output; surface a build failure embedded in the stream.
	data, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(data), "\"error\"") || strings.Contains(string(data), "errorDetail") {
		return fmt.Errorf("driver: image build failed: %s", lastLine(string(data)))
	}
	return nil
}

// tarDir builds a tar archive of dir suitable for a Docker build context.
func tarDir(dir string) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if info.IsDir() {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		return s[i+1:]
	}
	return s
}
