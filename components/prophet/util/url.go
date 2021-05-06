package util

import (
	"net/url"
	"strings"
)

// ParseUrls parse url
func ParseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, err
		}

		urls = append(urls, *u)
	}

	return urls, nil
}
