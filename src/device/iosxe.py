"""IOS-XE install-mode discovery, parsing, and compatibility policy."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Tuple


SUPPORTED_PLATFORMS = {
    "catalyst9200",
    "cat9200",
    "c9200",
    "catalyst9300",
    "cat9300",
    "c9300",
}

IMAGE_FAMILIES = {
    "c9200": ("cat9k_lite_iosxe",),
    "c9200l": ("cat9k_lite_iosxe",),
    "c9300l": ("cat9k_iosxe",),
    "c9300lm": ("cat9k_iosxe",),
    "c9300": ("cat9k_iosxe",),
    "c9300x": ("cat9k_iosxe",),
}


@dataclass(frozen=True, order=True)
class IOSXEVersion:
    """Comparable IOS-XE version."""

    major: int
    minor: int
    maintenance: int
    rebuild: str = ""

    @classmethod
    def parse(cls, value: str) -> "IOSXEVersion":
        match = re.search(
            r"(?<!\d)(\d+)\.(\d+)\.(\d+)([a-zA-Z]*)(?!\w)",
            str(value),
        )
        if not match:
            raise ValueError(f"Invalid IOS-XE version: {value!r}")
        return cls(
            major=int(match.group(1)),
            minor=int(match.group(2)),
            maintenance=int(match.group(3)),
            rebuild=match.group(4).lower(),
        )

    def __str__(self) -> str:
        return (
            f"{self.major}.{self.minor}.{self.maintenance}{self.rebuild}"
        )


@dataclass(frozen=True)
class InstallFacts:
    """Observed IOS-XE software and platform state."""

    current_version: str
    model: str
    serial: str
    system_image: str
    boot_target: str
    manual_boot: Optional[bool]
    boot_settings: List[Dict[str, Any]]
    install_mode: bool
    stack_members: Optional[int]
    install_summary_valid: bool
    install_states: Dict[str, List[str]]
    free_bytes: int
    total_bytes: int
    raw: Dict[str, str]

    def to_dict(self, *, include_raw: bool = False) -> Dict[str, Any]:
        """Return the parsed facts, without the raw CLI text by default.

        ``raw`` holds the full output of five show commands including
        ``dir flash:``. These facts are written into device metadata at several
        points in the workflow, so carrying the raw text along would store the
        same tens of kilobytes over and over in the state database and in every
        audit event. Callers that want it ask for it, and the orchestrator
        writes it to disk once per collection instead.
        """
        data = asdict(self)
        if not include_raw:
            data.pop("raw", None)
        return data


@dataclass(frozen=True)
class CompatibilityCheck:
    """One pre-upgrade compatibility decision."""

    name: str
    passed: bool
    message: str


@dataclass(frozen=True)
class CompatibilityReport:
    """Aggregate compatibility decision for a device."""

    passed: bool
    checks: List[CompatibilityCheck]

    @property
    def failures(self) -> List[str]:
        return [check.message for check in self.checks if not check.passed]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "checks": [asdict(check) for check in self.checks],
            "failures": self.failures,
        }


def parse_install_facts(outputs: Dict[str, str]) -> InstallFacts:
    """Parse upgrade-relevant facts from IOS-XE show-command output."""
    show_version = outputs.get("show version", "")
    show_boot = outputs.get("show boot", "")
    show_switch = outputs.get("show switch", "")
    show_install = outputs.get("show install summary", "")
    show_flash = outputs.get("dir flash:", "")

    current_version = _first_match(
        show_version,
        (
            r"Cisco IOS XE Software,\s*Version\s+([^\s,]+)",
            r"Cisco IOS Software.*?Version\s+([^\s,]+)",
        ),
        default="Unknown",
    )
    model = _first_match(
        show_version,
        (
            r"Model [Nn]umber\s*:\s*(\S+)",
            r"\b(C9[23]00[A-Z0-9-]*)\b",
        ),
        default="Unknown",
    )
    serial = _first_match(
        show_version,
        (r"Processor board ID\s+(\S+)",),
        default="Unknown",
    )
    system_image = _first_match(
        show_version,
        (r'System image file is\s+"([^"]+)"',),
        default="Unknown",
    )
    boot_settings = parse_next_boot_settings_all(show_boot)
    primary_boot = (
        boot_settings[0]
        if boot_settings
        else {
            "boot_target": "Unknown",
            "manual_boot": None,
        }
    )
    boot_target = str(primary_boot["boot_target"])
    manual_boot = primary_boot["manual_boot"]
    total_bytes, free_bytes = parse_flash_space(show_flash)
    install_states = parse_install_summary(show_install)
    install_summary_valid = _install_summary_is_valid(
        show_install,
        install_states,
    )
    install_mode = _is_packages_conf_path(system_image)

    return InstallFacts(
        current_version=current_version,
        model=model,
        serial=serial,
        system_image=system_image,
        boot_target=boot_target,
        manual_boot=manual_boot,
        boot_settings=boot_settings,
        install_mode=install_mode,
        stack_members=parse_stack_members(show_switch),
        install_summary_valid=install_summary_valid,
        install_states=install_states,
        free_bytes=free_bytes,
        total_bytes=total_bytes,
        raw=dict(outputs),
    )


def parse_flash_space(output: str) -> Tuple[int, int]:
    """Return total and free bytes from `dir flash:` output."""
    patterns = (
        r"(\d+)\s+bytes total\s+\((\d+)\s+bytes free\)",
        r"(\d+)\s+bytes total.*?(\d+)\s+bytes free",
    )
    for pattern in patterns:
        match = re.search(pattern, output, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return int(match.group(1)), int(match.group(2))
    return 0, 0


def parse_stack_members(output: str) -> Optional[int]:
    """Count members in `show switch`, returning unknown when unparseable."""
    members = {
        match.group(1)
        for line in output.splitlines()
        if (
            match := re.match(
                r"^\s*\*?\s*(\d+)\s+(?:Active|Standby|Member|Ready|Provisioned)\b",
                line,
                flags=re.IGNORECASE,
            )
        )
    }
    return len(members) if members else None


def parse_install_summary(output: str) -> Dict[str, List[str]]:
    """Parse image/package states from `show install summary`."""
    states: Dict[str, List[str]] = {
        "I": [],
        "U": [],
        "C": [],
        "D": [],
    }
    for line in output.splitlines():
        match = re.match(
            r"^\s*[A-Z0-9_-]+\s+([IUCD])\s+(.+?)\s*$",
            line,
            flags=re.IGNORECASE,
        )
        if match:
            states[match.group(1).upper()].append(match.group(2).strip())
    return states


def _install_summary_is_valid(
    output: str,
    states: Dict[str, List[str]],
) -> bool:
    error_markers = (
        "% invalid",
        "% authorization",
        "% authentication",
        "command authorization failed",
        "permission denied",
    )
    lowered = output.lower()
    unknown_state_rows = [
        line
        for line in output.splitlines()
        if (
            match := re.match(
                r"^\s*[A-Z0-9_-]+\s+([A-Z])\s+\S+",
                line,
                flags=re.IGNORECASE,
            )
        )
        and match.group(1).upper() not in states
    ]
    return bool(output.strip()) and not unknown_state_rows and not any(
        marker in lowered for marker in error_markers
    ) and any(states.values())


def normalize_device_path(image_path: str, filesystem: str = "flash:") -> str:
    """Return a canonical device-side image path."""
    if image_path.lower().startswith(filesystem.lower()):
        return f"{filesystem}{image_path[len(filesystem):]}"
    normalized = image_path.replace("\\", "/")
    return f"{filesystem}{PurePosixPath(normalized).name}"


def image_file_name(image_path: str) -> str:
    """Return just the image filename from a device or host path.

    ``Path(...).name`` does not split a Windows path when the orchestrator runs
    on Linux - which it must, because pyATS/Genie is not supported on native
    Windows - so backslashes and any device filesystem prefix are stripped
    here before the name is taken.
    """
    normalized = re.sub(
        r"^[a-zA-Z0-9_-]+:",
        "",
        str(image_path).replace("\\", "/"),
    )
    return PurePosixPath(normalized).name


def image_family(image_path: str) -> str:
    """Extract the IOS-XE image family prefix."""
    return image_file_name(image_path).lower().split(".", 1)[0]


def expected_image_families(model: str) -> Tuple[str, ...]:
    """Return known image family prefixes for a Catalyst model."""
    normalized = re.sub(r"[^a-z0-9]", "", model.lower())
    for model_prefix in sorted(IMAGE_FAMILIES, key=len, reverse=True):
        if normalized.startswith(model_prefix):
            return IMAGE_FAMILIES[model_prefix]
    return ()


def evaluate_compatibility(
    *,
    device: Dict[str, Any],
    facts: InstallFacts,
    image_size_bytes: Optional[int],
    config: Dict[str, Any],
) -> CompatibilityReport:
    """Evaluate install-mode safety gates before staging or activation."""
    checks: List[CompatibilityCheck] = []
    platform = str(device.get("platform", "")).lower().replace("-", "")
    target_version_text = str(device.get("target_version", ""))
    image_path = str(device.get("image_path", ""))
    safety = config.get("safety", {})
    pre_checks = config.get("pre_checks", {})

    _add_check(
        checks,
        "supported_platform",
        platform in SUPPORTED_PLATFORMS,
        f"Platform {device.get('platform')!r} is supported",
        f"Platform {device.get('platform')!r} is not supported",
    )
    _add_check(
        checks,
        "install_mode",
        facts.install_mode,
        "Device is booted in install mode",
        "Running system image is not flash:packages.conf",
    )
    boot_settings_complete = (
        facts.stack_members is not None
        and len(facts.boot_settings) == facts.stack_members
    )
    safe_boot_targets = boot_settings_complete and all(
        _is_packages_conf_path(str(setting.get("boot_target", "")))
        for setting in facts.boot_settings
    )
    manual_boot_disabled = boot_settings_complete and all(
        setting.get("manual_boot") is False
        for setting in facts.boot_settings
    )
    _add_check(
        checks,
        "install_boot_target",
        safe_boot_targets,
        "Every member next boots flash:packages.conf",
        "Could not prove flash:packages.conf as every member's next boot target",
    )
    _add_check(
        checks,
        "manual_boot_disabled",
        manual_boot_disabled,
        "Manual boot is disabled for every member",
        "Could not prove Manual Boot = no for every member",
    )
    allow_stacks = bool(safety.get("allow_stacks", False))
    stack_state_known = facts.stack_members is not None
    _add_check(
        checks,
        "standalone_device",
        stack_state_known
        and (facts.stack_members == 1 or allow_stacks),
        f"Detected {facts.stack_members} switch member(s)",
        (
            "Could not prove standalone switch state"
            if not stack_state_known
            else f"Detected {facts.stack_members} switch members; stacks are disabled"
        ),
    )
    _add_check(
        checks,
        "install_summary",
        facts.install_summary_valid and bool(facts.install_states.get("C")),
        "Install summary contains a committed running image",
        "Could not prove a committed current image from show install summary",
    )
    _add_check(
        checks,
        "clean_install_state",
        facts.install_summary_valid
        and not facts.install_states.get("U")
        and not facts.install_states.get("D"),
        "No existing uncommitted install operation",
        "Existing activated/deactivated uncommitted packages require resolution",
    )

    try:
        current_version = IOSXEVersion.parse(facts.current_version)
        target_version = IOSXEVersion.parse(target_version_text)
        parsed_versions = True
    except ValueError:
        current_version = target_version = None
        parsed_versions = False
    _add_check(
        checks,
        "versions_parse",
        parsed_versions,
        f"Version transition {facts.current_version} -> {target_version_text}",
        "Current or target IOS-XE version could not be parsed",
    )
    if parsed_versions and current_version and target_version:
        allow_downgrade = bool(safety.get("allow_downgrade", False))
        _add_check(
            checks,
            "upgrade_direction",
            target_version >= current_version or allow_downgrade,
            "Version direction is allowed",
            "Downgrades require safety.allow_downgrade=true",
        )
        supported_major_versions = {
            int(value)
            for value in safety.get("supported_major_versions", [16, 17])
        }
        _add_check(
            checks,
            "supported_release_family",
            current_version.major in supported_major_versions
            and target_version.major in supported_major_versions,
            "Current and target release families are allowed",
            "Current or target major release is outside the configured policy",
        )

    # Parse the version from the filename alone. Directory names routinely
    # contain a release number of their own (``.../17.03.05/cat9k...bin``) and
    # IOSXEVersion.parse takes the first match it finds.
    try:
        image_version = IOSXEVersion.parse(image_file_name(image_path))
        target_matches_image = (
            target_version is not None and image_version == target_version
        )
    except ValueError:
        target_matches_image = False
    _add_check(
        checks,
        "image_version",
        target_matches_image,
        "Image filename matches the requested target version",
        "Image filename does not match target_version",
    )

    expected_families = expected_image_families(facts.model)
    actual_family = image_family(image_path)
    family_matches = (
        actual_family in expected_families if expected_families else False
    )
    _add_check(
        checks,
        "image_family",
        family_matches,
        f"Image family {actual_family} matches {facts.model}",
        (
            f"Image family {actual_family!r} is not known to match "
            f"model {facts.model!r}"
        ),
    )

    minimum_mb = int(pre_checks.get("minimum_free_space_mb", 2048))
    buffer_multiplier = float(
        pre_checks.get("image_space_buffer_multiplier", 1.5)
    )
    required_bytes = minimum_mb * 1024 * 1024
    if image_size_bytes is not None:
        required_bytes = max(
            required_bytes,
            int(image_size_bytes * buffer_multiplier),
        )
    _add_check(
        checks,
        "flash_space",
        facts.free_bytes >= required_bytes,
        (
            f"Flash has {facts.free_bytes / (1024 ** 3):.2f} GiB free; "
            f"{required_bytes / (1024 ** 3):.2f} GiB required"
        ),
        (
            f"Insufficient flash: {facts.free_bytes / (1024 ** 3):.2f} GiB "
            f"free, {required_bytes / (1024 ** 3):.2f} GiB required"
        ),
    )

    if safety.get("require_console_confirmation", False):
        confirmed = _as_bool(device.get("console_verified"))
        _add_check(
            checks,
            "console_access",
            confirmed,
            "Out-of-band console access was confirmed",
            "Out-of-band console access must be confirmed in inventory",
        )

    return CompatibilityReport(
        passed=all(check.passed for check in checks),
        checks=checks,
    )


def target_version_matches(current: str, target: str) -> bool:
    """Compare IOS-XE versions exactly while tolerating zero padding."""
    try:
        return IOSXEVersion.parse(current) == IOSXEVersion.parse(target)
    except ValueError:
        return False


def parse_next_boot_settings(output: str) -> Tuple[str, Optional[bool]]:
    """Return the next-reload boot target and manual-boot setting."""
    settings = parse_next_boot_settings_all(output)
    if not settings:
        return "Unknown", None
    return str(settings[0]["boot_target"]), settings[0]["manual_boot"]


def parse_next_boot_settings_all(output: str) -> List[Dict[str, Any]]:
    """Return next-reload settings for every switch section."""
    switch_headers = list(
        re.finditer(
            r"^\s*Switch\s+(\d+)\s*$",
            output,
            flags=re.IGNORECASE | re.MULTILINE,
        )
    )
    if not switch_headers:
        target, manual_boot = _parse_boot_settings_section(output)
        return [
            {
                "member": None,
                "boot_target": target,
                "manual_boot": manual_boot,
            }
        ]

    settings: List[Dict[str, Any]] = []
    for index, header in enumerate(switch_headers):
        end = (
            switch_headers[index + 1].start()
            if index + 1 < len(switch_headers)
            else len(output)
        )
        target, manual_boot = _parse_boot_settings_section(
            output[header.end() : end]
        )
        settings.append(
            {
                "member": int(header.group(1)),
                "boot_target": target,
                "manual_boot": manual_boot,
            }
        )
    return settings


def _parse_boot_settings_section(output: str) -> Tuple[str, Optional[bool]]:
    next_reload = re.search(
        r"Boot Variables on next reload\s*:(.*)$",
        output,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if next_reload:
        relevant = next_reload.group(1)
    elif re.search(r"Current Boot Variables\s*:", output, re.IGNORECASE):
        relevant = ""
    else:
        relevant = output
    target = _first_match(
        relevant,
        (
            r"BOOT path-list\s*:\s*(\S+)",
            r"BOOT variable\s*=\s*(\S+)",
            r"BOOT\s*=\s*(\S+)",
        ),
        default="Unknown",
    ).rstrip(";,")
    manual_match = re.search(
        r"Manual Boot\s*[:=]\s*(yes|no)\b",
        relevant,
        flags=re.IGNORECASE,
    )
    manual_boot = (
        manual_match.group(1).lower() == "yes" if manual_match else None
    )
    return target, manual_boot


def _is_packages_conf_path(value: str) -> bool:
    normalized = str(value).strip().strip("\"'").replace("\\", "/")
    return bool(
        re.fullmatch(
            r"(?:[a-zA-Z0-9_-]+:)?/?packages\.conf",
            normalized,
            flags=re.IGNORECASE,
        )
    )


def _first_match(
    value: str,
    patterns: Iterable[str],
    *,
    default: str,
) -> str:
    for pattern in patterns:
        match = re.search(pattern, value, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
    return default


def _add_check(
    checks: List[CompatibilityCheck],
    name: str,
    passed: bool,
    success_message: str,
    failure_message: str,
) -> None:
    checks.append(
        CompatibilityCheck(
            name=name,
            passed=bool(passed),
            message=success_message if passed else failure_message,
        )
    )


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}
