import hashlib
import json
import re
from typing import Any


CATEGORIES = {
    "OOM",
    "TIMEOUT",
    "PERMISSION",
    "DATA_ISSUE",
    "CODE_ERROR",
    "CLUSTER",
    "NETWORK",
    "CONFIGURATION",
    "UNKNOWN",
}

RECOMMENDATIONS = {
    "OOM": (
        "REVIEW_RESOURCE_OR_PARTITIONING",
        "Review executor/driver memory, data skew, and partition sizing before changing job resources.",
    ),
    "TIMEOUT": (
        "REVIEW_RUNTIME_AND_TIMEOUT",
        "Inspect the slow task and recent runtime trend; change timeout settings only after confirming the bottleneck.",
    ),
    "PERMISSION": (
        "ROUTE_TO_DATA_OWNER",
        "Identify the affected resource and route an access request to its owner; do not grant permissions automatically.",
    ),
    "DATA_ISSUE": (
        "REVIEW_INPUT_DATA",
        "Inspect the failing input and upstream contract; quarantine or correct data through the owning pipeline.",
    ),
    "CODE_ERROR": (
        "ROUTE_TO_CODE_OWNER",
        "Review the task code and stack trace in the linked run; prepare a source-controlled fix for human review.",
    ),
    "CLUSTER": (
        "REVIEW_CLUSTER_EVENTS",
        "Inspect cluster events, runtime compatibility, and recent configuration changes before rerunning.",
    ),
    "NETWORK": (
        "REVIEW_DEPENDENCY_CONNECTIVITY",
        "Check the dependency endpoint and transient network evidence; any retry must be approved and bounded.",
    ),
    "CONFIGURATION": (
        "ROUTE_TO_JOB_OWNER",
        "Compare the current job/task configuration with the last known-good configuration.",
    ),
    "UNKNOWN": (
        "HUMAN_REVIEW_REQUIRED",
        "Review the full task output and job configuration; no automatic action is authorized.",
    ),
}

_CATEGORY_ALIASES = {
    "DATA ISSUE": "DATA_ISSUE",
    "CODE ERROR": "CODE_ERROR",
}

_CATEGORY_PATTERNS = (
    ("OOM", r"\bout of memory\b|\bmemoryerror\b|\bjava heap space\b|\bexit code 137\b"),
    ("TIMEOUT", r"\btimeout\b|\btimed out\b|\bdeadline exceeded\b"),
    ("PERMISSION", r"\bpermission denied\b|\bunauthorized\b|\bforbidden\b|\baccess denied\b"),
    ("DATA_ISSUE", r"\bmalformed\b|\binvalid input\b|\bschema mismatch\b|\bcorrupt\b"),
    (
        "CODE_ERROR",
        r"\bstack trace\b|\btraceback\b|\bexception\b|\bsyntaxerror\b|"
        r"\bzerodivisionerror\b|\bdivision by zero\b",
    ),
    ("CLUSTER", r"\bcluster\b|\bexecutor lost\b|\bdriver unavailable\b|\bnode failure\b"),
    ("NETWORK", r"\bconnection reset\b|\bconnection refused\b|\bnetwork\b|\bdns\b"),
    ("CONFIGURATION", r"\bconfiguration\b|\bmissing parameter\b|\bnot configured\b"),
)

_REDACTIONS = (
    (re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]+=*"), "Bearer [REDACTED]"),
    (
        re.compile(
            r"(?i)\b(password|passwd|token|secret|api[_-]?key|access[_-]?key)"
            r"(\s*[:=]\s*)([^\s,;]+)"
        ),
        r"\1\2[REDACTED]",
    ),
    (
        re.compile(r"(?i)(https?://[^/\s:@]+):([^@\s/]+)@"),
        r"\1:[REDACTED]@",
    ),
)


def make_incident_key(workspace_id: str, job_id: int, run_id: int) -> str:
    raw_key = f"{workspace_id}:{job_id}:{run_id}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def redact_failure_text(value: str, max_chars: int = 8000) -> str:
    text = value or ""
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text[:max_chars]


def classify_failure(value: str, existing_category: str | None = None) -> str:
    if existing_category:
        normalized = existing_category.strip().upper().replace("-", "_")
        normalized = _CATEGORY_ALIASES.get(normalized, normalized)
        if normalized in CATEGORIES:
            return normalized

    text = (value or "").lower()
    for category, pattern in _CATEGORY_PATTERNS:
        if re.search(pattern, text):
            return category
    return "UNKNOWN"


def make_recommendation(category: str) -> dict[str, Any]:
    normalized = category if category in CATEGORIES else "UNKNOWN"
    action, description = RECOMMENDATIONS[normalized]
    return {
        "category": normalized,
        "action": action,
        "description": description,
        "automatic_action_authorized": False,
    }


def parse_diagnosis(raw_response: str) -> dict[str, str]:
    value = raw_response.strip()
    if value.startswith("```"):
        value = re.sub(r"^```(?:json)?\s*|\s*```$", "", value, flags=re.IGNORECASE)
    parsed = json.loads(value)
    category = str(parsed.get("category", "")).strip().upper().replace("-", "_")
    category = _CATEGORY_ALIASES.get(category, category)
    if category not in CATEGORIES:
        raise ValueError(f"Unsupported diagnosis category: {category!r}")
    return {
        "category": category,
        "root_cause": str(parsed.get("root_cause", "")).strip(),
        "candidate_fix": str(parsed.get("candidate_fix", "")).strip(),
    }


def validate_proposal(category: str, recommendation: dict[str, Any]) -> str:
    expected = make_recommendation(category)
    if recommendation.get("action") != expected["action"]:
        return "POLICY_REVIEW_REQUIRED"
    if recommendation.get("automatic_action_authorized") is not False:
        return "POLICY_REVIEW_REQUIRED"
    return "PROPOSAL_POLICY_VALIDATED"


def render_review_packet(
    *,
    job_name: str,
    job_id: int,
    run_id: int,
    run_page_url: str,
    category: str,
    root_cause: str,
    recommendation: dict[str, Any],
    candidate_fix: str,
) -> str:
    safe_url = run_page_url or "Not available"
    return (
        f"# Failed job review: {job_name or job_id}\n\n"
        f"- Workspace job ID: `{job_id}`\n"
        f"- Failed run ID: `{run_id}`\n"
        f"- Run: {safe_url}\n"
        f"- Classification: **{category}**\n"
        f"- Automated production action: **Not authorized**\n\n"
        f"## Root-cause assessment\n{root_cause or 'Requires human investigation.'}\n\n"
        f"## Recommended next step\n{recommendation['description']}\n\n"
        f"## Candidate fix\n{candidate_fix or 'No code-level fix generated; inspect the linked run and source repository.'}\n\n"
        "## Validation and approval\n"
        "This packet is a review artifact only. No job was retried, no generated code was "
        "executed, and no GitHub pull request was opened. A source-repository mapping and "
        "isolated test environment are required before validating or submitting a code change.\n"
    )
