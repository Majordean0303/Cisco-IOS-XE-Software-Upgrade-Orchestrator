"""Credential retrieval without persisting secrets in job state or inventory."""

from __future__ import annotations

import getpass
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class Credentials:
    """Credentials required for an IOS-XE or jump-host connection."""

    username: str
    password: str
    secret: str = ""

    def to_dict(self) -> Dict[str, str]:
        return {
            "username": self.username,
            "password": self.password,
            "secret": self.secret,
        }


class CredentialManager:
    """Resolve credentials from keyring, environment, then configuration."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        load_dotenv()

    def get_device_credentials(
        self,
        hostname: Optional[str] = None,
    ) -> Dict[str, str]:
        credentials_config = self.config.get("credentials", {})
        service = credentials_config.get(
            "credman_service_devices",
            "cisco-ios-xe-orchestrator-devices",
        )
        if hostname and credentials_config.get("per_device_credentials", False):
            service = f"{service}:{hostname}"

        credentials = self._from_keyring(service, credentials_config)
        if credentials is None:
            credentials = self._from_environment(
                username_variable="DEVICE_USERNAME",
                password_variable="DEVICE_PASSWORD",
                secret_variable="DEVICE_ENABLE_PASSWORD",
            )
        if credentials is None:
            credentials = Credentials(
                username=str(credentials_config.get("username", "") or ""),
                password=str(credentials_config.get("password", "") or ""),
                secret=str(
                    credentials_config.get("enable_password", "") or ""
                ),
            )
        return credentials.to_dict()

    def get_jumphost_credentials(self) -> Dict[str, str]:
        credentials_config = self.config.get("credentials", {})
        service = credentials_config.get(
            "credman_service_jumphost",
            "cisco-ios-xe-orchestrator-jumphost",
        )
        credentials = self._from_keyring(service, credentials_config)
        if credentials is None:
            credentials = self._from_environment(
                username_variable="JUMP_HOST_USERNAME",
                password_variable="JUMP_HOST_PASSWORD",
            )
        jump_config = self.config.get("connection", {}).get("jump_host", {})
        if credentials is None:
            credentials = Credentials(
                username=str(jump_config.get("username", "") or ""),
                password=str(jump_config.get("password", "") or ""),
            )
        return credentials.to_dict()

    def _from_keyring(
        self,
        service: str,
        credentials_config: Dict[str, Any],
    ) -> Optional[Credentials]:
        if not credentials_config.get("use_credential_manager", True):
            return None
        try:
            import keyring
            from keyring.errors import KeyringError
        except ImportError:
            return None

        try:
            payload = keyring.get_password(service, "credentials")
            if payload:
                decoded = json.loads(payload)
                return Credentials(
                    username=str(decoded.get("username", "")),
                    password=str(decoded.get("password", "")),
                    secret=str(decoded.get("secret", "")),
                )

            configured_username = str(
                credentials_config.get("username", "") or ""
            )
            if configured_username:
                password = keyring.get_password(service, configured_username)
                if password:
                    return Credentials(
                        username=configured_username,
                        password=password,
                        secret=str(
                            credentials_config.get("enable_password", "") or ""
                        ),
                    )
        except (KeyringError, json.JSONDecodeError):
            return None
        return None

    @staticmethod
    def _from_environment(
        *,
        username_variable: str,
        password_variable: str,
        secret_variable: Optional[str] = None,
    ) -> Optional[Credentials]:
        username = os.getenv(username_variable, "")
        password = os.getenv(password_variable, "")
        if not username or not password:
            return None
        return Credentials(
            username=username,
            password=password,
            secret=os.getenv(secret_variable, "") if secret_variable else "",
        )


def setup_credentials_interactive(config: Dict[str, Any]) -> None:
    """Store device or jump-host credentials in the system keyring."""
    try:
        import keyring
    except ImportError as error:
        raise RuntimeError(
            "The keyring package is required to store credentials"
        ) from error

    credentials_config = config.get("credentials", {})
    kind = input("Credential type [device/jumphost]: ").strip().lower()
    if kind not in {"device", "jumphost"}:
        raise ValueError("Credential type must be 'device' or 'jumphost'")

    service_key = (
        "credman_service_devices"
        if kind == "device"
        else "credman_service_jumphost"
    )
    service_default = (
        "cisco-ios-xe-orchestrator-devices"
        if kind == "device"
        else "cisco-ios-xe-orchestrator-jumphost"
    )
    service = credentials_config.get(service_key, service_default)
    if kind == "device":
        hostname = input(
            "Device hostname (leave blank for global credentials): "
        ).strip()
        if hostname:
            service = f"{service}:{hostname}"
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")
    secret = (
        getpass.getpass("Enable password (optional): ")
        if kind == "device"
        else ""
    )
    if not username or not password:
        raise ValueError("Username and password are required")

    keyring.set_password(
        service,
        "credentials",
        json.dumps(
            {
                "username": username,
                "password": password,
                "secret": secret,
            }
        ),
    )
    print(f"Credentials stored in keyring service: {service}")
