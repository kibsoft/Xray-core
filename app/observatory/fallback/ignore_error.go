package fallback

import (
	goerrors "errors"
	"strings"
	"syscall"

	"github.com/xtls/xray-core/common/errors"
	"golang.org/x/net/http2"
)

const (
	IgnoreErrorInternal = "internalError"
	IgnoreErrorWSASend  = "wsasend"
)

// WSAECONNABORTED is Windows error 10053: local software aborted the socket.
const wsaEConnAborted syscall.Errno = 10053

// NormalizeIgnoreErrors maps JSON/config names to canonical ignore tokens.
// Empty input means ignore nothing. Unknown names are rejected.
func NormalizeIgnoreErrors(names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(names))
	out := make([]string, 0, len(names))
	for _, name := range names {
		canonical, err := canonicalizeIgnoreError(name)
		if err != nil {
			return nil, err
		}
		if canonical == "" {
			continue
		}
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		out = append(out, canonical)
	}
	return out, nil
}

func canonicalizeIgnoreError(name string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "":
		return "", nil
	case "internalerror", "internal_error", "internal-error":
		return IgnoreErrorInternal, nil
	case "wsasend", "localabort", "local_abort", "local-abort":
		return IgnoreErrorWSASend, nil
	default:
		return "", errors.New("unknown ignoreErrors value: ", name, " (want internalError or wsasend)")
	}
}

func isHTTP2InternalError(err error) bool {
	if err == nil {
		return false
	}
	var streamErr http2.StreamError
	if goerrors.As(err, &streamErr) && streamErr.Code == http2.ErrCodeInternal {
		return true
	}
	var connErr http2.ConnectionError
	if goerrors.As(err, &connErr) && http2.ErrCode(connErr) == http2.ErrCodeInternal {
		return true
	}
	msg := err.Error()
	if !strings.Contains(msg, "INTERNAL_ERROR") {
		return false
	}
	return strings.Contains(msg, "stream error") ||
		strings.Contains(msg, "connection error") ||
		strings.Contains(msg, "received from peer")
}

func isLocalAbortError(err error) bool {
	if err == nil {
		return false
	}
	var errno syscall.Errno
	if goerrors.As(err, &errno) && errno == wsaEConnAborted {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "wsasend") ||
		strings.Contains(msg, "aborted by the software in your host machine")
}

func errorMatchesIgnoreKind(kind string, err error) bool {
	switch kind {
	case IgnoreErrorInternal:
		return isHTTP2InternalError(err)
	case IgnoreErrorWSASend:
		return isLocalAbortError(err)
	default:
		return false
	}
}
