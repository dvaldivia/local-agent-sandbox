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
	"fmt"
	"strings"
)

// splitArgs tokenizes a command string using POSIX-shell-like rules for
// whitespace, single quotes, double quotes, and backslash escapes — enough to
// match the reference runtime's shlex.split for the commands the SDK sends.
// It performs NO shell interpretation (no globbing, pipes, redirection, or
// variable expansion); metacharacters become literal argument bytes.
func splitArgs(s string) ([]string, error) {
	var args []string
	var cur strings.Builder
	inSingle, inDouble, hasToken := false, false, false

	flush := func() {
		if hasToken {
			args = append(args, cur.String())
			cur.Reset()
			hasToken = false
		}
	}

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inSingle:
			if c == '\'' {
				inSingle = false
			} else {
				cur.WriteByte(c)
			}
		case inDouble:
			switch {
			case c == '"':
				inDouble = false
			case c == '\\' && i+1 < len(s) && (s[i+1] == '"' || s[i+1] == '\\' || s[i+1] == '$' || s[i+1] == '`'):
				i++
				cur.WriteByte(s[i])
			default:
				cur.WriteByte(c)
			}
		default:
			switch c {
			case '\'':
				inSingle = true
				hasToken = true
			case '"':
				inDouble = true
				hasToken = true
			case '\\':
				if i+1 < len(s) {
					i++
					cur.WriteByte(s[i])
					hasToken = true
				}
			case ' ', '\t', '\n', '\r':
				flush()
			default:
				cur.WriteByte(c)
				hasToken = true
			}
		}
	}
	if inSingle || inDouble {
		return nil, fmt.Errorf("unbalanced quotes")
	}
	flush()
	return args, nil
}
