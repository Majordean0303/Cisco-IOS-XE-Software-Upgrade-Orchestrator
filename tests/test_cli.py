import unittest
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from orchestrator import cli


class CliSafetyTests(unittest.TestCase):
    def test_missing_configuration_fails_closed(self):
        result = CliRunner().invoke(
            cli,
            [
                "--config",
                "config/does-not-exist.yaml",
                "status",
            ],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Configuration file not found", result.output)

    @patch("orchestrator.build_engine")
    def test_dry_run_cannot_be_applied_to_existing_upgrade(self, build_engine):
        engine = MagicMock()
        engine.store.get_job.return_value = {
            "job_id": "job-1",
            "dry_run": False,
        }
        build_engine.return_value = engine

        result = CliRunner().invoke(
            cli,
            [
                "--config",
                "config/config.example.yaml",
                "upgrade",
                "--job-id",
                "job-1",
                "--dry-run",
                "--yes",
            ],
        )

        self.assertEqual(result.exit_code, 2)
        self.assertIn(
            "--dry-run cannot be applied to an existing non-dry-run job",
            result.output,
        )
        engine.run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
