package prune

import (
	"path"
	"strings"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
)

func cleanKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == "/" {
		return ""
	}

	return strings.TrimPrefix(path.Clean("/"+value), "/")
}

func joins(prefix, name string) string {
	return cleanKey(prefix + "/" + name)
}

func containsPrefix(parent, child string) bool {
	parent = cleanKey(parent)
	child = cleanKey(child)

	if parent == "" {
		return true
	}

	return child == parent || strings.HasPrefix(child, parent+"/")
}

func overlaps(a, b string) bool {
	return containsPrefix(a, b) || containsPrefix(b, a)
}

func matchingProtection(prefix string, protections []domain.Protection) *domain.Protection {
	for i := range protections {
		if overlaps(prefix, protections[i].Prefix) {
			protection := protections[i]
			return &protection
		}
	}

	return nil
}
