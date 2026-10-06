package monitor

import (
	"net/url"
	"regexp"
	"strings"
)

// urlInText finds URLs inside error text. net/http wraps failures in
// *url.Error, whose message embeds the full request URL, and feed URLs can
// carry API keys in the path or query.
var urlInText = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"'<>]+`)

// redactURLs replaces every URL in text with scheme://host, dropping userinfo,
// path, query, and fragment so credentials never reach logs.
func redactURLs(text string) string {
	return urlInText.ReplaceAllStringFunc(text, func(raw string) string {
		// Trim punctuation that belongs to the sentence, not the URL.
		trimmed := strings.TrimRight(raw, ".,;:)]}")
		suffix := raw[len(trimmed):]
		u, err := url.Parse(trimmed)
		if err != nil || u.Host == "" {
			return "<url>" + suffix
		}
		return u.Scheme + "://" + u.Host + suffix
	})
}

// redactedError carries the original error for errors.Is/As (retry and
// classification depend on it) while printing text with URLs redacted.
type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

// redactSourceError hides feed URL secrets in err's text without changing how
// it unwraps. Errors that mention no URL are returned unchanged.
func redactSourceError(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	if redacted := redactURLs(text); redacted != text {
		return &redactedError{msg: redacted, err: err}
	}
	return err
}

// sourceLogLabel identifies a feed in logs without its path or query.
func sourceLogLabel(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Host == "" {
		return sourceFailureHost(rawURL)
	}
	return u.Scheme + "://" + u.Host
}
