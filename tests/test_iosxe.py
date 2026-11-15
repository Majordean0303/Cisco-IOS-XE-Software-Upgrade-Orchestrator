import unittest

from src.device.iosxe import (
    IOSXEVersion,
    evaluate_compatibility,
    expected_image_families,
    image_family,
    normalize_device_path,
    parse_flash_space,
    parse_install_facts,
    parse_next_boot_settings,
    parse_next_boot_settings_all,
    parse_stack_members,
    target_version_matches,
)


OUTPUTS = {
    "show version": (
        "Cisco IOS XE Software, Version 17.9.4a\n"
        'System image file is "flash:packages.conf"\n'
        "Model Number : C9300-48P\n"
        "Processor board ID ABC123"
    ),
    "show boot": (
        "BOOT path-list : flash:packages.conf\n"
        "Manual Boot : no"
    ),
    "show switch": "*1 Active Ready",
    "show install summary": "IMG   C    17.9.4a",
    "dir flash:": "11353194496 bytes total (5648769024 bytes free)",
}


class IOSXEParsingTests(unittest.TestCase):
    def test_parses_install_mode_facts(self):
        facts = parse_install_facts(OUTPUTS)

        self.assertTrue(facts.install_mode)
        self.assertEqual(facts.current_version, "17.9.4a")
        self.assertEqual(facts.model, "C9300-48P")
        self.assertEqual(facts.stack_members, 1)
        self.assertEqual(facts.install_states["C"], ["17.9.4a"])
        self.assertTrue(facts.install_summary_valid)
        self.assertFalse(facts.manual_boot)
        self.assertEqual(facts.free_bytes, 5648769024)

    def test_flash_parser_handles_parentheses(self):
        total, free = parse_flash_space(
            "11353194496 bytes total (5648769024 bytes free)"
        )

        self.assertEqual(total, 11353194496)
        self.assertEqual(free, 5648769024)

    def test_versions_compare_with_zero_padding(self):
        self.assertTrue(target_version_matches("17.09.04a", "17.9.4a"))
        self.assertGreater(
            IOSXEVersion.parse("17.12.1"),
            IOSXEVersion.parse("17.9.4a"),
        )

    def test_bundle_runtime_fails_even_when_next_boot_uses_packages_conf(self):
        outputs = dict(OUTPUTS)
        outputs["show version"] = OUTPUTS["show version"].replace(
            "flash:packages.conf", "flash:cat9k_iosxe.17.09.04a.SPA.bin"
        )

        facts = parse_install_facts(outputs)

        self.assertFalse(facts.install_mode)
        self.assertEqual(facts.boot_target, "flash:packages.conf")

    def test_unknown_stack_output_fails_closed(self):
        self.assertIsNone(parse_stack_members(""))
        self.assertIsNone(parse_stack_members("% Invalid input detected"))

    def test_invalid_install_summary_fails_compatibility(self):
        outputs = dict(OUTPUTS)
        outputs["show install summary"] = "% Invalid input detected"
        facts = parse_install_facts(outputs)

        report = evaluate_compatibility(
            device={
                "hostname": "sw1",
                "platform": "catalyst9300",
                "target_version": "17.12.4",
                "image_path": "/images/cat9k_iosxe.17.12.04.SPA.bin",
            },
            facts=facts,
            image_size_bytes=None,
            config={"pre_checks": {"minimum_free_space_mb": 1}},
        )

        failures = {check.name for check in report.checks if not check.passed}
        self.assertFalse(facts.install_summary_valid)
        self.assertIn("install_summary", failures)
        self.assertIn("clean_install_state", failures)

    def test_smu_rows_participate_in_install_state_gates(self):
        outputs = dict(OUTPUTS)
        outputs["show install summary"] = (
            "IMG C 17.9.4a\n"
            "SMU U flash:planned-fix.smu.bin"
        )
        facts = parse_install_facts(outputs)

        report = evaluate_compatibility(
            device={
                "hostname": "sw1",
                "platform": "catalyst9300",
                "target_version": "17.12.4",
                "image_path": "/images/cat9k_iosxe.17.12.04.SPA.bin",
            },
            facts=facts,
            image_size_bytes=None,
            config={"pre_checks": {"minimum_free_space_mb": 1}},
        )

        self.assertEqual(
            facts.install_states["U"],
            ["flash:planned-fix.smu.bin"],
        )
        failures = {check.name for check in report.checks if not check.passed}
        self.assertIn("clean_install_state", failures)

    def test_unknown_install_state_row_fails_closed(self):
        outputs = dict(OUTPUTS)
        outputs["show install summary"] = (
            "IMG C 17.9.4a\n"
            "SMU X flash:unknown-state.smu.bin"
        )

        facts = parse_install_facts(outputs)

        self.assertFalse(facts.install_summary_valid)

    def test_device_flash_prefix_is_canonicalized(self):
        self.assertEqual(
            normalize_device_path(
                "FLASH:cat9k_iosxe.17.12.04.SPA.bin"
            ),
            "flash:cat9k_iosxe.17.12.04.SPA.bin",
        )

    def test_next_reload_boot_settings_override_current_values(self):
        target, manual_boot = parse_next_boot_settings(
            "Current Boot Variables:\n"
            "BOOT variable = flash:packages.conf;\n"
            "Manual Boot = no\n"
            "Boot Variables on next reload:\n"
            "BOOT variable = flash:cat9k_iosxe.17.12.04.SPA.bin;\n"
            "Manual Boot = yes"
        )

        self.assertEqual(
            target,
            "flash:cat9k_iosxe.17.12.04.SPA.bin",
        )
        self.assertTrue(manual_boot)

    def test_manual_boot_or_bundle_next_boot_fails_compatibility(self):
        outputs = dict(OUTPUTS)
        outputs["show boot"] = (
            "Current Boot Variables:\n"
            "BOOT variable = flash:packages.conf;\n"
            "Manual Boot = no\n"
            "Boot Variables on next reload:\n"
            "BOOT variable = flash:cat9k_iosxe.17.12.04.SPA.bin;\n"
            "Manual Boot = yes"
        )
        facts = parse_install_facts(outputs)

        report = evaluate_compatibility(
            device={
                "hostname": "sw1",
                "platform": "catalyst9300",
                "target_version": "17.12.4",
                "image_path": "/images/cat9k_iosxe.17.12.04.SPA.bin",
            },
            facts=facts,
            image_size_bytes=None,
            config={"pre_checks": {"minimum_free_space_mb": 1}},
        )

        failures = {check.name for check in report.checks if not check.passed}
        self.assertIn("install_boot_target", failures)
        self.assertIn("manual_boot_disabled", failures)

    def test_stack_boot_settings_are_validated_for_every_member(self):
        outputs = dict(OUTPUTS)
        outputs["show switch"] = "*1 Active Ready\n 2 Member Ready"
        outputs["show boot"] = (
            "Switch 1\n"
            "Current Boot Variables:\n"
            "BOOT variable = flash:packages.conf;\n"
            "Boot Variables on next reload:\n"
            "BOOT variable = flash:packages.conf;\n"
            "Manual Boot = no\n"
            "Switch 2\n"
            "Current Boot Variables:\n"
            "BOOT variable = flash:packages.conf;\n"
            "Boot Variables on next reload:\n"
            "BOOT variable = flash:cat9k_iosxe.17.12.04.SPA.bin;\n"
            "Manual Boot = yes"
        )
        facts = parse_install_facts(outputs)

        report = evaluate_compatibility(
            device={
                "hostname": "stack1",
                "platform": "catalyst9300",
                "target_version": "17.12.4",
                "image_path": "/images/cat9k_iosxe.17.12.04.SPA.bin",
            },
            facts=facts,
            image_size_bytes=None,
            config={
                "pre_checks": {"minimum_free_space_mb": 1},
                "safety": {"allow_stacks": True},
            },
        )

        self.assertEqual(len(parse_next_boot_settings_all(outputs["show boot"])), 2)
        failures = {check.name for check in report.checks if not check.passed}
        self.assertIn("install_boot_target", failures)
        self.assertIn("manual_boot_disabled", failures)

    def test_pre_staged_image_family_strips_flash_prefix(self):
        self.assertEqual(
            image_family("flash:cat9k_iosxe.17.12.04.SPA.bin"),
            "cat9k_iosxe",
        )
        self.assertEqual(
            expected_image_families("C9300L-48P-4G"),
            ("cat9k_iosxe",),
        )

    def test_compatibility_rejects_bundle_mode_and_wrong_image_family(self):
        outputs = dict(OUTPUTS)
        outputs["show version"] = OUTPUTS["show version"].replace(
            "flash:packages.conf", "flash:cat9k_iosxe.17.09.04a.SPA.bin"
        )
        outputs["show boot"] = (
            "BOOT path-list : flash:cat9k_iosxe.17.09.04a.SPA.bin"
        )
        facts = parse_install_facts(outputs)
        report = evaluate_compatibility(
            device={
                "hostname": "sw1",
                "platform": "catalyst9300",
                "target_version": "17.12.4",
                "image_path": "/images/cat9k_lite_iosxe.17.12.04.SPA.bin",
                "console_verified": True,
            },
            facts=facts,
            image_size_bytes=500_000_000,
            config={
                "pre_checks": {"minimum_free_space_mb": 2048},
                "safety": {},
            },
        )

        failures = {check.name for check in report.checks if not check.passed}
        self.assertIn("install_mode", failures)
        self.assertIn("image_family", failures)


if __name__ == "__main__":
    unittest.main()
