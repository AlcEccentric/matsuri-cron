package utils

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// BuildQueryParams constructs a URL-encoded query string from a map of parameters.
// Each key-value pair in the map is converted to "key=value" format, with both
// the key and value being URL-encoded. The pairs are then joined by "&".
func BuildQueryParams(params map[string]string) string {
	var queryParams []string
	for key, value := range params {
		queryParams = append(queryParams, fmt.Sprintf("%s=%s", url.QueryEscape(key), url.QueryEscape(value)))
	}
	return strings.Join(queryParams, "&")
}

// JoinSlice joins elements of a slice into a single string, separated by the specified delimiter.
func JoinSlice[T any](slice []T, delimeter string) string {
	var parts []string
	for _, v := range slice {
		parts = append(parts, fmt.Sprint(v))
	}
	return strings.Join(parts, delimeter)
}

func CreateDirectoryIfNotExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}
	return nil
}

func LocalFileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func ReadJSONFile(path string, v interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open JSON file %s: %w", path, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(v); err != nil {
		return fmt.Errorf("failed to decode JSON file %s: %w", path, err)
	}
	return nil
}

func WriteJSONFile(file *os.File, data interface{}, pretty bool) error {
	encoder := json.NewEncoder(file)
	if pretty {
		encoder.SetIndent("", "  ") // Pretty print with indentation
	}

	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to write JSON to file: %w", err)
	}
	return nil
}

// Generic function to check if slice1 is a subset of slice2
func IsSubset[T comparable](slice1, slice2 []T) bool {
	elementMap := make(map[T]struct{})

	for _, v := range slice2 {
		elementMap[v] = struct{}{}
	}

	for _, v := range slice1 {
		if _, exists := elementMap[v]; !exists {
			return false
		}
	}

	return true
}
