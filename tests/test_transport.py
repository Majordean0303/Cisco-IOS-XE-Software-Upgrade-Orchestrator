import unittest
from unittest.mock import patch

from src.transport import (
    execute_install_activate,
    execute_install_cleanup,
)


class FakeConnection:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.sent = []

    def send_command_timing(self, command, **kwargs):
        del kwargs
        self.sent.append(command)
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


class EnableConnection(FakeConnection):
    def __init__(self, responses):
        super().__init__(responses)
        self.enabled = False
        self.enable_calls = 0

    def check_enable_mode(self):
        return self.enabled

    def enable(self):
        self.enable_calls += 1
        self.enabled = True


class ChannelConnection(FakeConnection):
    def __init__(self, responses, channel_reads):
        super().__init__(responses)
        self.channel_reads = iter(channel_reads)

    def read_channel(self):
        return next(self.channel_reads)


class FakeHost:
    def __init__(self, connection):
        self.connection = connection

    def get_connection(self, name, config):
        del name, config
        return self.connection


class FakeTask:
    def __init__(self, connection):
        self.host = FakeHost(connection)
        self.nornir = type("Nornir", (), {"config": object()})()


class InstallPromptTests(unittest.TestCase):
    def test_activate_treats_disconnect_after_reload_confirmation_as_success(self):
        connection = FakeConnection(
            [
                "This operation requires a reload of the system. "
                "Do you want to proceed? [y/n]",
                EOFError("device reloading"),
            ]
        )

        result = execute_install_activate(FakeTask(connection))

        self.assertFalse(result.failed)
        self.assertTrue(result.result["reload_initiated"])
        self.assertEqual(connection.sent, ["install activate", "y"])

    def test_save_prompt_does_not_count_as_reload_confirmation(self):
        connection = FakeConnection(
            [
                "System configuration has been modified. [y/n/q]",
                "Operation cancelled",
            ]
        )

        result = execute_install_activate(FakeTask(connection))

        self.assertTrue(result.failed)
        self.assertFalse(result.result["reload_initiated"])

    def test_cleanup_answers_delete_prompt_without_expecting_reload(self):
        connection = FakeConnection(
            [
                "Do you want to remove the above files? [y/n]",
                "SUCCESS: install_remove",
            ]
        )

        result = execute_install_cleanup(FakeTask(connection))

        self.assertFalse(result.failed)
        self.assertFalse(result.result["reload_initiated"])
        self.assertEqual(
            connection.sent,
            ["install remove inactive", "y"],
        )

    def test_install_operation_enters_enable_mode(self):
        connection = EnableConnection(["SUCCESS: install_commit"])

        from src.transport import execute_install_commit

        result = execute_install_commit(FakeTask(connection))

        self.assertFalse(result.failed)
        self.assertEqual(connection.enable_calls, 1)

    @patch("src.transport.time.sleep", return_value=None)
    def test_install_polling_continues_until_deadline(self, _sleep):
        connection = ChannelConnection(
            ["Install operation is still running"],
            [""] * 25 + ["\nSUCCESS: install_remove"],
        )

        result = execute_install_cleanup(
            FakeTask(connection),
            max_wait=5,
        )

        self.assertFalse(result.failed)


class TransferErrorTests(unittest.TestCase):
    def test_channel_refused_error_explains_scp_server_requirement(self):
        from src.transport import _describe_transfer_error

        class ChannelException(Exception):
            pass

        message = _describe_transfer_error(
            ChannelException("ChannelException(1, 'Administratively prohibited')")
        )

        self.assertIn("ip scp server enable", message)
        self.assertIn("privilege level 15", message)
        self.assertIn("Administratively prohibited", message)

    def test_other_errors_pass_through_unchanged(self):
        from src.transport import _describe_transfer_error

        message = _describe_transfer_error(TimeoutError("connection timed out"))

        self.assertEqual(message, "connection timed out")


class SendCommandConnection:
    """Fake Netmiko connection that records ``send_command`` kwargs."""

    def __init__(self, response):
        self.response = response
        self.commands = []
        self.kwargs = []

    def send_command(self, command, **kwargs):
        self.commands.append(command)
        self.kwargs.append(kwargs)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class VerifyImageHashTests(unittest.TestCase):
    def test_verify_image_hash_passes_read_timeout_to_send_command(self):
        from src.transport import verify_image_hash

        expected = "a" * 32
        connection = SendCommandConnection(
            f"verify /md5 (flash:image.bin) = {expected}"
        )

        result = verify_image_hash(
            FakeTask(connection),
            image_name="image.bin",
            expected_md5=expected,
            read_timeout=2400,
        )

        self.assertFalse(result.failed)
        self.assertTrue(result.result["hash_match"])
        self.assertEqual(result.result["device_md5"], expected)
        self.assertEqual(connection.commands, ["verify /md5 flash:image.bin"])
        self.assertEqual(connection.kwargs[0]["read_timeout"], 2400)
        self.assertFalse(connection.kwargs[0]["use_textfsm"])

    def test_verify_image_hash_default_read_timeout_exceeds_netmiko_default(self):
        from src.transport import verify_image_hash

        expected = "b" * 32
        connection = SendCommandConnection(
            f"verify /md5 (flash:image.bin) = {expected}"
        )

        verify_image_hash(
            FakeTask(connection),
            image_name="image.bin",
            expected_md5=expected,
        )

        # Netmiko's built-in default is 100 seconds, which is what caused the
        # original ReadTimeout during staging. Anything materially larger is
        # acceptable; assert we are well above that threshold.
        self.assertGreaterEqual(connection.kwargs[0]["read_timeout"], 600)

    def test_verify_image_hash_reports_read_timeout_error(self):
        from src.transport import verify_image_hash

        connection = SendCommandConnection(
            RuntimeError("Pattern not detected: 'SW#' in output.")
        )

        result = verify_image_hash(
            FakeTask(connection),
            image_name="image.bin",
            expected_md5="c" * 32,
        )

        self.assertTrue(result.failed)
        self.assertFalse(result.result["hash_match"])
        self.assertIn("Pattern not detected", result.result["error"])


if __name__ == "__main__":
    unittest.main()
