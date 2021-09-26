// Copyright 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/iancoleman/orderedmap"
)

type OrderedMap = orderedmap.OrderedMap

var npFields = map[string]bool{
	"level":  false,
	"ts":     false,
	"msg":    false,
	"logger": false,
	"case":   false,
	"caller": false,
}

var levelMap = map[string]string{
	"info":  "I",
	"debug": "D",
	"warn":  "W",
	"error": "E",
	"fatal": "C",
}

func getTimestamp(m *OrderedMap) (string, bool) {
	v, ok := m.Get("ts")
	if !ok {
		return "", false
	}
	tsv, ok := v.(string)
	if !ok {
		return "", false
	}

	layouts := []string{
		"2021-09-26 22:39:33.403",
		"2021-09-26T22:39:33.401+0800",
	}
	for _, layout := range layouts {
		ts, err := time.Parse(layout, tsv)
		if err == nil {
			return ts.Format(layouts[0]), true
		}
	}
	return tsv, true
}

func getMessage(m *OrderedMap) (string, bool) {
	v, ok := m.Get("msg")
	if !ok {
		return "", false
	}
	vv, ok := v.(string)
	return vv, ok
}

func isJsonString(s string) bool {
	return s[0] == '{' && s[len(s)-1] == '}'
}

func getLineMap(s string) (*OrderedMap, error) {
	m := orderedmap.New()
	if err := m.UnmarshalJSON([]byte(s)); err != nil {
		return nil, err
	}
	return m, nil
}

func getLevel(m *OrderedMap) (string, bool) {
	if v, ok := m.Get("level"); ok {
		if c, ok := levelMap[v.(string)]; ok {
			return c, true
		}
		return v.(string), false
	}
	return "", false
}

func toString(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func toHumanReadable(m *OrderedMap) (string, bool) {
	level, ok1 := getLevel(m)
	ts, ok2 := getTimestamp(m)
	msg, ok3 := getMessage(m)
	if !ok1 || !ok2 || !ok3 {
		return "", false
	}

	var values []string
	keys := m.Keys()
	for _, key := range keys {
		if _, ok := npFields[key]; !ok {
			v, ok := m.Get(key)
			if !ok {
				panic("ordered map corrupted?")
			}
			values = append(values, fmt.Sprintf("%s:%s", key, toString(v)))
		}
	}
	var result string
	if len(values) > 0 {
		result = fmt.Sprintf("%s %s %s, %s",
			ts, level, msg, strings.Join(values, ", "))
	} else {
		result = fmt.Sprintf("%s %s %s", ts, level, msg)
	}
	return result, true
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if isJsonString(line) {
			if m, err := getLineMap(line); err == nil {
				if formatted, ok := toHumanReadable(m); ok {
					fmt.Println(formatted)
					continue
				}
			}
		}
		fmt.Println(line)
	}
}
