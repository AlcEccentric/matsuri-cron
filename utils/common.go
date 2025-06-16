package utils

import (
	"fmt"
	"net/url"
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
