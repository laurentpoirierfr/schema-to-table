package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// parseKV parses "k=v,k2=v2" flag payloads into a map.
func parseKV(s string) (map[string]string, error) {
	m := map[string]string{}
	s = strings.TrimSpace(s)
	if s == "" {
		return m, nil
	}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(pair, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid pair %q (want key=value)", pair)
		}
		m[k] = strings.TrimSpace(v)
	}
	return m, nil
}

// orDefault returns s, or fallback when s is empty.
func orDefault(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}

// dataFiles lists *.json files in dir, sorted for deterministic runs.
func dataFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read data dir: %w", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no *.json files in %s", dir)
	}
	return files, nil
}
