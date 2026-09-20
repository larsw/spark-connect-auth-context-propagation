"""Tests for turning connectionOptions into PyIceberg properties.

These deliberately import only ``polaris_iceberg.config`` and
``polaris_iceberg.catalog``, never ``polaris_iceberg.metadata``: the first two
need nothing but pyiceberg, so the mapping can be tested on the host without the
OpenMetadata ingestion framework installed.
"""
import pytest

from polaris_iceberg.catalog import redact
from polaris_iceberg.config import PolarisIcebergConfig

POLARIS = {
    "uri": "http://polaris:8181/api/catalog",
    "warehouse": "poc_catalog",
    "clientId": "openmetadata",
    "clientSecret": "openmetadata-secret",
    "oauth2ServerUri": "http://keycloak:8080/realms/spark/protocol/openid-connect/token",
    "scope": "openid profile",
    "databaseName": "polaris",
}


def build(**overrides):
    options = {**POLARIS, **overrides}
    return PolarisIcebergConfig.from_options(options)


class TestOAuth2ClientCredentials:
    """The Polaris setup: tokens come from Keycloak, not from Polaris itself."""

    def test_a_token_url_selects_the_oauth2_auth_manager(self):
        properties = build().pyiceberg_properties()

        assert properties["auth"] == {
            "type": "oauth2",
            "oauth2": {
                "client_id": "openmetadata",
                "client_secret": "openmetadata-secret",
                "token_url": POLARIS["oauth2ServerUri"],
                "scope": "openid profile",
            },
        }

    def test_the_deprecated_credential_flow_is_not_used_as_well(self):
        # Both would "work", but `credential` is the legacy flow PyIceberg drops
        # in 1.0, and it posts to the catalog's own endpoint rather than Keycloak.
        properties = build().pyiceberg_properties()

        assert "credential" not in properties
        assert "oauth2-server-uri" not in properties

    def test_without_a_token_url_it_falls_back_to_the_catalog_endpoint(self):
        properties = build(oauth2ServerUri=None).pyiceberg_properties()

        assert properties["credential"] == "openmetadata:openmetadata-secret"
        assert "auth" not in properties


class TestOptionParsing:
    def test_the_uri_is_required(self):
        with pytest.raises(ValueError, match="missing 'uri'"):
            PolarisIcebergConfig.from_options({"warehouse": "poc_catalog"})

    def test_a_half_supplied_client_credential_is_rejected(self):
        with pytest.raises(ValueError, match="clientSecret"):
            PolarisIcebergConfig.from_options(
                {"uri": "http://polaris:8181/api/catalog", "clientId": "openmetadata"}
            )

    @pytest.mark.parametrize("spelling", ["clientId", "client_id", "client-id", "CLIENTID"])
    def test_option_names_are_matched_however_they_are_spelled(self, spelling):
        config = PolarisIcebergConfig.from_options(
            {
                "uri": "http://polaris:8181/api/catalog",
                spelling: "openmetadata",
                "clientSecret": "s3cret",
            }
        )

        assert config.client_id == "openmetadata"

    def test_a_trailing_slash_on_the_uri_is_dropped(self):
        assert build(uri="http://polaris:8181/api/catalog/").uri == (
            "http://polaris:8181/api/catalog"
        )

    def test_the_database_name_defaults_when_not_given(self):
        assert PolarisIcebergConfig.from_options(
            {"uri": "http://polaris:8181/api/catalog"}
        ).database_name == "default"


class TestPassthrough:
    def test_dotted_options_reach_pyiceberg_untouched(self):
        properties = build(**{
            "s3.endpoint": "http://minio:9000",
            "header.X-Correlation-Id": "abc-123",
        }).pyiceberg_properties()

        assert properties["s3.endpoint"] == "http://minio:9000"
        assert properties["header.X-Correlation-Id"] == "abc-123"

    def test_passthrough_wins_over_a_derived_value(self):
        # So a catalog needing something this connector gets wrong stays usable.
        properties = build(**{"rest.signing-region": "eu-north-1"}).pyiceberg_properties()

        assert properties["rest.signing-region"] == "eu-north-1"


class TestNoneValuesAreDropped:
    def test_unset_keys_are_absent_rather_than_none(self):
        # PyIceberg tests membership, not truthiness, so a present-but-None key
        # reads as configured and then fails on use.
        properties = PolarisIcebergConfig.from_options(
            {"uri": "http://polaris:8181/api/catalog"}
        ).pyiceberg_properties()

        assert None not in properties.values()
        assert set(properties) == {"uri"}


class TestRedaction:
    def test_secrets_never_reach_a_log_line(self):
        properties = build().pyiceberg_properties()

        rendered = repr(redact(properties))

        assert "openmetadata-secret" not in rendered
        assert "***" in rendered

    def test_the_legacy_credential_is_redacted_too(self):
        properties = build(oauth2ServerUri=None).pyiceberg_properties()

        assert "openmetadata-secret" not in repr(redact(properties))


class TestPyIcebergAcceptsWhatWeBuild:
    """Guards the property names against a PyIceberg upgrade renaming them."""

    def test_the_auth_block_constructs_pyicebergs_oauth2_manager(self):
        from pyiceberg.catalog.rest.auth import AuthManagerFactory

        auth = build().pyiceberg_properties()["auth"]

        # Exactly how RestCatalog._create_session dispatches it. A wrong key name
        # would be a TypeError here rather than a 401 at ingestion time.
        manager = AuthManagerFactory.create(auth["type"], auth[auth["type"]])

        assert manager is not None

    def test_the_flat_property_names_are_the_ones_pyiceberg_reads(self):
        import pyiceberg.catalog.rest as rest

        from polaris_iceberg import config

        assert config.URI == rest.URI
        assert config.WAREHOUSE == rest.WAREHOUSE_LOCATION
        assert config.CREDENTIAL == rest.CREDENTIAL
        assert config.TOKEN == rest.TOKEN
        assert config.SCOPE == rest.SCOPE
        assert config.OAUTH2_SERVER_URI == rest.OAUTH2_SERVER_URI
        assert config.SIGV4 == rest.SIGV4
        assert config.SIGV4_REGION == rest.SIGV4_REGION
        assert config.SIGV4_SERVICE == rest.SIGV4_SERVICE
