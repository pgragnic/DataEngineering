"""Smoke tests — run without a live DB or Anthropic API key."""

from unittest.mock import MagicMock, patch

import pytest


class TestAgentClientSmoke:
    def test_call_claude_json_parses_json(self):
        mock_resp = MagicMock()
        mock_resp.content = [MagicMock(text='{"key": "value"}')]

        with patch("app.agents.client._client") as mock_client:
            mock_client.messages.create.return_value = mock_resp
            from app.agents.client import call_claude_json
            result = call_claude_json(system="test", user_content=[{"type": "text", "text": "hi"}])
        assert result == {"key": "value"}

    def test_call_claude_json_strips_fences(self):
        mock_resp = MagicMock()
        mock_resp.content = [MagicMock(text='```json\n{"answer": 42}\n```')]

        with patch("app.agents.client._client") as mock_client:
            mock_client.messages.create.return_value = mock_resp
            from app.agents.client import call_claude_json
            result = call_claude_json(system="test", user_content=[{"type": "text", "text": "hi"}])
        assert result == {"answer": 42}


class TestPreparationAgent:
    def test_generate_checklist_returns_sections(self):
        mock_checklist = {
            "referential": "ISO 9001:2015",
            "scope_summary": "Test",
            "sections": [
                {
                    "id": "S1",
                    "title": "Documents",
                    "points": [
                        {
                            "id": "S1.P1",
                            "question": "Q1?",
                            "expected_evidence": "Doc",
                            "norm_reference": "ISO 9001 §7.5",
                        }
                    ],
                }
            ],
        }
        with (
            patch("app.agents.preparation.call_claude_json", return_value=mock_checklist),
            patch("app.agents.preparation.retrieve_norm_context", return_value=[]),
        ):
            from app.agents.preparation import generate_checklist
            result = generate_checklist(
                client_name="ALPHA",
                site_name="Tours",
                referential="ISO 9001:2015",
                scope="Achats",
            )
        assert "sections" in result
        assert len(result["sections"]) >= 1


class TestCaptureAgent:
    def test_classify_constat_returns_classification(self):
        mock_result = {
            "reformulated_text": "NC détectée.",
            "classification": "nc_majeure",
            "norm_reference": "ISO 9001 §7.1.4",
            "norm_excerpt": "Extrait...",
            "suggested_evidence": "Photo",
            "suggested_action": "Corriger.",
        }
        with (
            patch("app.agents.capture.call_claude_json", return_value=mock_result),
            patch("app.agents.capture.retrieve_norm_context", return_value=[]),
        ):
            from app.agents.capture import classify_constat
            result, chunks = classify_constat(raw_text="sortie obstruée")
        assert result["classification"] == "nc_majeure"
        assert "reformulated_text" in result


class TestDocxGen:
    def test_generate_docx_returns_bytes(self):
        from app.docx_gen.report import generate_docx
        report_structure = {
            "executive_summary": "Audit OK.",
            "conformity_summary": {"conforme": 1, "observation": 0, "nc_mineure": 0, "nc_majeure": 0},
            "sections": [],
            "action_plan": [],
            "next_audit_recommendation": "Pas de suivi nécessaire.",
        }
        result = generate_docx(
            inspection={"client_name": "ALPHA", "site_name": "Tours", "auditor_name": "Test", "referential": "ISO 9001"},
            report_structure=report_structure,
        )
        assert isinstance(result, bytes)
        assert len(result) > 0
