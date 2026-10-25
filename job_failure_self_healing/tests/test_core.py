import unittest

from job_failure_self_healing.core import (
    classify_failure,
    make_incident_key,
    make_recommendation,
    parse_diagnosis,
    redact_failure_text,
    validate_proposal,
)


class FailureTriageTests(unittest.TestCase):
    def test_incident_key_is_stable_and_scoped_to_run(self):
        first = make_incident_key("7405611780316702", 12, 34)
        self.assertEqual(first, make_incident_key("7405611780316702", 12, 34))
        self.assertNotEqual(first, make_incident_key("7405611780316702", 12, 35))
        self.assertNotEqual(first, make_incident_key("different", 12, 34))

    def test_classifies_known_failure_categories(self):
        self.assertEqual(classify_failure("Executor lost: Java heap space"), "OOM")
        self.assertEqual(classify_failure("Permission denied for table"), "PERMISSION")
        self.assertEqual(classify_failure("ZeroDivisionError: division by zero"), "CODE_ERROR")
        self.assertEqual(classify_failure("Malformed input: required field missing"), "DATA_ISSUE")
        self.assertEqual(classify_failure("Task timed out"), "TIMEOUT")
        self.assertEqual(classify_failure("unrecognized failure"), "UNKNOWN")

    def test_valid_existing_category_is_normalized(self):
        self.assertEqual(classify_failure("unrelated text", "Data Issue"), "DATA_ISSUE")
        self.assertEqual(classify_failure("unrelated text", "made_up"), "UNKNOWN")

    def test_recommendation_never_authorizes_automatic_action(self):
        for category in ("OOM", "TIMEOUT", "PERMISSION", "UNKNOWN"):
            proposal = make_recommendation(category)
            self.assertFalse(proposal["automatic_action_authorized"])
            self.assertEqual(validate_proposal(category, proposal), "PROPOSAL_POLICY_VALIDATED")

    def test_policy_rejects_changed_action_or_auto_authorization(self):
        proposal = make_recommendation("NETWORK")
        proposal["action"] = "RETRY_NOW"
        self.assertEqual(validate_proposal("NETWORK", proposal), "POLICY_REVIEW_REQUIRED")
        proposal = make_recommendation("NETWORK")
        proposal["automatic_action_authorized"] = True
        self.assertEqual(validate_proposal("NETWORK", proposal), "POLICY_REVIEW_REQUIRED")

    def test_parses_only_supported_diagnosis_categories(self):
        parsed = parse_diagnosis(
            '{"category":"TIMEOUT","root_cause":"Slow dependency","candidate_fix":"Review timeout"}'
        )
        self.assertEqual(parsed["category"], "TIMEOUT")
        with self.assertRaises(ValueError):
            parse_diagnosis('{"category":"RUN_AUTOMATICALLY"}')

    def test_redacts_common_credentials(self):
        text = "Authorization: Bearer abc.def and api_key=secret123"
        redacted = redact_failure_text(text)
        self.assertNotIn("abc.def", redacted)
        self.assertNotIn("secret123", redacted)


if __name__ == "__main__":
    unittest.main()
