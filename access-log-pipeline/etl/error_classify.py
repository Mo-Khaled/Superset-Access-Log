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


# ---- signature: remove only what is specific to ONE occurrence, keep what is informative ----
#
# Removed:  numbers, UUIDs and long hex ids, timestamps/dates, IP addresses, quoted VALUES (text with digits, spaces
#           or more than 30 characters, such as literals in "invalid input syntax for integer: 'abc 123'"), and
#           very long bracketed lists.
# Kept:     HTTP status codes ("http 422" and "http 500" stay different), identifier names in quotes or short lists
#           ("column "foo" does not exist", "columns missing in dataset: ['title', 'owner']"), exception class names,
#           words. Two different missing columns are two different problems, so they stay two signatures.
# The cost: a message built around many different identifiers produces many signatures. That is visible in the Top
# Error Messages table, and the raw message is always kept, so nothing is lost by erring on the side of keeping.
_UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I)
_TIMESTAMP = re.compile(r"\b\d{4}-\d{2}-\d{2}([t ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?(z|[+-]\d{2}:?\d{2})?\b", re.I)
_IPV4 = re.compile(r"\b\d{1,3}(\.\d{1,3}){3}(:\d+)?\b")
_HEX_ID = re.compile(r"\b(?=[0-9a-f]*\d)(?=[0-9a-f]*[a-f])[0-9a-f]{8,}\b", re.I)  # hex run that mixes digits and letters
_QUOTED = re.compile(r"'([^']*)'|\"([^\"]*)\"|`([^`]*)`")
_GROUPED = re.compile(r"\[[^\]]{60,}\]|\{[^}]{60,}\}|\([^)]{60,}\)")
_NUMBER = re.compile(r"\b\d+(\.\d+)?\b")
_SPACE = re.compile(r"\s+")
_STATUS_CONTEXT = re.compile(r"(http|https|status|code|error|errno)[ :=#]*$")


def _quoted(m):
    value = m.group(1) if m.group(1) is not None else (m.group(2) if m.group(2) is not None else m.group(3))
    quote = m.group(0)[0]
    looks_like_identifier = re.fullmatch(r"[\w.\-]{1,30}", value or "") and not re.search(r"\d", value or "")
    return m.group(0) if looks_like_identifier else f"{quote}?{quote}"


def _number(m, text):
    value = m.group(0)
    if len(value) == 3 and value[0] in "1345" and _STATUS_CONTEXT.search(text[max(0, m.start() - 14):m.start()]):
        return value  # an HTTP status code is information, not noise
    return "N"


def signature(message, limit=120):
    """The message with the parts specific to one occurrence removed, so the same error groups together while different
    errors stay apart. See the notes above for exactly what is removed and what is kept."""
    s = (message or "").strip().lower()
    s = _UUID.sub("<uuid>", s)
    s = _TIMESTAMP.sub("<ts>", s)
    s = _IPV4.sub("<ip>", s)
    s = _HEX_ID.sub("<id>", s)
    s = _QUOTED.sub(_quoted, s)
    s = _GROUPED.sub("[..]", s)
    s = _NUMBER.sub(lambda m: _number(m, s), s)
    s = _SPACE.sub(" ", s).strip(" :;,.")
    return s[:limit] or "(no message)"


_PATH_ID = re.compile(r"/(\d+|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(?=/|$)", re.I)


def path_template(path):
    """'/api/v1/chart/166/data' -> '/api/v1/chart/{id}/data': the endpoint, without which record it was about."""
    return _PATH_ID.sub("/{id}", path or "")


def one_line(text, limit=300):
    """Messages are stored as a single trimmed line."""
    return _SPACE.sub(" ", str(text or "")).strip()[:limit]


def row_category_and_signature(source, error_type, status_code, request_path, message):
    """Category and signature of an error row, from the fields that are STORED on it. Every writer (ETL, ingest, backfill,
    reclassify) uses this one function, so a row gets the same answer wherever it is computed and a change to the rules can
    be applied to old rows by recomputing from the table."""
    message = message or ""
    if source == "gunicorn":
        # one signature per kind of incident, whatever pid or request it involved
        category = "worker_failure" if error_type == "worker_killed" else classify(message)
        return category, "gunicorn " + (error_type or "error").replace("_", " ")
    if source == "server":
        category = classify(message, status_code)
        return category, signature(f"http {status_code} {path_template(request_path)}: {message or 'no message'}")
    return classify(message, status_code), signature(message)
