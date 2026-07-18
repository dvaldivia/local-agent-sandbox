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
	"reflect"
	"testing"
)

func TestSplitArgs(t *testing.T) {
	cases := []struct {
		in   string
		want []string
		err  bool
	}{
		{"echo hello", []string{"echo", "hello"}, false},
		{"  echo   hello  ", []string{"echo", "hello"}, false},
		{"sh -c 'exit 3'", []string{"sh", "-c", "exit 3"}, false},
		{`python -c "print('hi')"`, []string{"python", "-c", "print('hi')"}, false},
		{`echo "a b" c`, []string{"echo", "a b", "c"}, false},
		{`echo a\ b`, []string{"echo", "a b"}, false},
		{"echo a | wc -l", []string{"echo", "a", "|", "wc", "-l"}, false},
		{"", nil, false},
		{"'unterminated", nil, true},
		{`"unterminated`, nil, true},
		{`echo ''`, []string{"echo", ""}, false},
	}
	for _, c := range cases {
		got, err := splitArgs(c.in)
		if c.err {
			if err == nil {
				t.Errorf("splitArgs(%q): expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("splitArgs(%q): unexpected error %v", c.in, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("splitArgs(%q) = %#v, want %#v", c.in, got, c.want)
		}
	}
}
