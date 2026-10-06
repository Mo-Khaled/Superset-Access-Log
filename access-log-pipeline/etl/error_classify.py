"""
Cause-agnostic error classification, shared by every error source (browser events, server request errors, gunicorn,
Celery). We do not know in advance which failures production will produce, so nothing here assumes a particular cause:

  * the RAW message is always kept (`error_message`), so nothing is lost
  * a coarse CATEGORY (timeout, query_error, permission, ...) lets dashboards chart errors by kind
  * a SIGNATURE (the message with numbers, ids and quoted values removed) groups "the same" error that differs only in
    details, so an unknown cause shows up as a growing signature in the "Top error messages" table instead of being
    hidden as one-off text

Anything the rules do not recognise is categorised `unclassified`. That is a signal to look at the signature, and to add
a rule here; it is never dropped.
"""
import re

# Order matters: the first matching rule wins. Specific causes first, status-code fallbacks last.
_RULES = [
    ("timeout", re.compile(r"timeout|timed out|time limit|statement timeout|deadline exceeded|canceling statement", re.I)),
    ("worker_failure", re.compile(r"was sent sig|worker failed to boot|worker exiting|out of memory|\boom\b|segfault|killed", re.I)),
    ("connection", re.compile(
        r"connection (refused|reset|closed|aborted)|could not connect|max retries|name resolution|"
        r"failed to fetch|network ?error|econn|unable to connect|broken pipe|remote end closed", re.I)),
    ("permission", re.compile(r"access is denied|permission|not authorized|unauthorized|forbidden|csrf|session expired|login required", re.I)),
    # specific query problems first, so "column x does not exist" is a query error and not a "not found"
    ("query_error", re.compile(
        r"syntax error|(column|relation|table|schema|function) .*(does not exist|not found|missing)|columns missing|"
        r"no such (table|column)|unknown (table|column)|invalid (input|query|column|metric)|division by zero|"
        r"query (failed|error)|database error|operationalerror|programmingerror", re.I)),
    ("not_found", re.compile(r"not found|does not exist|404", re.I)),
    ("rate_limit", re.compile(r"too many requests|rate limit|429", re.I)),
    ("query_error", re.compile(r"\bsql\b|datasource|dataset", re.I)),
    ("data_error", re.compile(r"type ?error|value ?error|key ?error|cannot convert|could not convert|out of range|null value|unexpected data", re.I)),
]


def classify(message, status=None):
    """Coarse category for an error. `status` is the HTTP status code when the error came from a request."""
    text = message or ""
    for name, rx in _RULES:
        if rx.search(text):
            return name
    if status:
        if status in (401, 403):
            return "permission"
        if status == 404:
            return "not_found"
        if status == 429:
            return "rate_limit"
        if 400 <= status < 500:
            return "client_error"
        if status >= 500:
            return "server_error"
    return "unclassified"


_QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"|`[^`]*`")
_BRACKETS = re.compile(r"\[[^\]]*\]|\{[^}]*\}|\([^)]*\)")
_HEX_ID = re.compile(r"\b[0-9a-f]{8,}\b", re.I)
_NUMBER = re.compile(r"\d+(\.\d+)?")
_SPACE = re.compile(r"\s+")


def signature(message, limit=100):
    """The message with the variable parts removed, so errors that differ only in an id, a number or a quoted value group
    together. 'Columns missing in dataset: [title, owner]' and the same with other names give one signature."""
    s = (message or "").strip().lower()
    s = _QUOTED.sub("?", s)
    s = _BRACKETS.sub("[..]", s)
    s = _HEX_ID.sub("<id>", s)
    s = _NUMBER.sub("N", s)
    s = _SPACE.sub(" ", s).strip(" :;,.")
    return s[:limit] or "(no message)"


def one_line(text, limit=300):
    """Messages are stored as a single trimmed line."""
    return _SPACE.sub(" ", str(text or "")).strip()[:limit]
