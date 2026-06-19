"""Tests for IFS Cloud integration helpers."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from fabrictools.integrations.ifs.api import ifs_request_json, read_ifs_entity_with_token
from fabrictools.integrations.ifs.auth import IFSAuthManager
from fabrictools.integrations.ifs.client import IFSClient
from fabrictools.integrations.ifs.config import IFSConfig
from fabrictools.integrations.ifs.credentials import ifs_config_with_keyvault_secret
from fabrictools.integrations.ifs.errors import IFSError
from fabrictools.integrations.ifs.odata import build_entity_url, extract_entity_rows, extract_next_link


def _config() -> IFSConfig:
    return IFSConfig(
        host="https://ifs.example.com",
        client_id="client-id",
        client_secret="client-secret",
        layer="int",
        page_size=2,
    )


def test_build_entity_url_with_query_options() -> None:
    config = _config()
    url = build_entity_url(
        config,
        "ActivityService",
        "Activities",
        odata_filter="Company eq 'SPH'",
        select=["ActivityId", "Company"],
        top=100,
        skip=50,
        orderby="ActivityId desc",
    )
    assert url.startswith("https://ifs.example.com/int/ifsapplications/projection/v1/ActivityService.svc/Activities?")
    assert "$filter=Company%20eq%20'SPH'" in url
    assert "$select=ActivityId%2CCompany" in url
    assert "$top=100" in url
    assert "$skip=50" in url
    assert "$orderby=ActivityId%20desc" in url


def test_extract_entity_rows_and_next_link() -> None:
    payload = {
        "value": [{"ActivityId": 1}, {"ActivityId": 2}],
        "@odata.nextLink": "https://ifs.example.com/next",
    }
    assert extract_entity_rows(payload) == [{"ActivityId": 1}, {"ActivityId": 2}]
    assert extract_next_link(payload) == "https://ifs.example.com/next"


def test_ifs_error_from_http_response() -> None:
    body = json.dumps(
        {
            "error": {
                "code": "MAIN_CODE",
                "message": "Main message",
                "details": [{"code": "DETAIL", "message": "Detail message"}],
            }
        }
    )
    err = IFSError.from_http_response(400, body)
    assert err.status_code == 400
    assert err.error_code == "MAIN_CODE"
    assert str(err) == "Main message"
    assert err.details == [{"code": "DETAIL", "message": "Detail message"}]


@patch("fabrictools.integrations.ifs.auth.http_request_json")
def test_auth_manager_caches_token(mock_http_request_json: MagicMock) -> None:
    mock_http_request_json.return_value = {"access_token": "token-1", "expires_in": 3600}
    auth = IFSAuthManager(_config())

    assert auth.get_access_token() == "token-1"
    assert auth.get_access_token() == "token-1"
    mock_http_request_json.assert_called_once()


@patch("fabrictools.integrations.ifs.client.http_request_json")
@patch("fabrictools.integrations.ifs.client.IFSClient.get_access_token", return_value="token-1")
def test_client_fetch_all_with_skip_pagination(
    _mock_token: MagicMock,
    mock_http_request_json: MagicMock,
) -> None:
    mock_http_request_json.side_effect = [
        {"value": [{"id": 1}, {"id": 2}]},
        {"value": [{"id": 3}]},
    ]
    client = IFSClient(_config())
    rows = client.get_entity("ActivityService", "Activities", fetch_all=True)

    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert mock_http_request_json.call_count == 2


@patch("fabrictools.integrations.ifs.client.http_request_json")
@patch("fabrictools.integrations.ifs.client.IFSClient.get_access_token", return_value="token-1")
def test_client_fetch_all_with_next_link(
    _mock_token: MagicMock,
    mock_http_request_json: MagicMock,
) -> None:
    mock_http_request_json.side_effect = [
        {"value": [{"id": 1}], "@odata.nextLink": "https://ifs.example.com/next"},
        {"value": [{"id": 2}]},
    ]
    client = IFSClient(_config())
    rows = client.get_entity("ActivityService", "Activities", fetch_all=True)

    assert rows == [{"id": 1}, {"id": 2}]
    assert mock_http_request_json.call_count == 2


@patch("fabrictools.integrations.ifs.credentials._get_keyvault_secret", return_value="kv-secret")
def test_ifs_config_with_keyvault_secret(mock_get_secret: MagicMock) -> None:
    config = ifs_config_with_keyvault_secret(
        host="https://ifs.example.com",
        client_id="client-id",
        keyvault_url="https://kv.vault.azure.net/",
        client_secret_name="ifs-client-secret",
        layer="main",
    )
    assert config.client_secret == "kv-secret"
    assert config.layer == "main"
    mock_get_secret.assert_called_once_with("https://kv.vault.azure.net/", "ifs-client-secret")


@pytest.mark.parametrize("layer", ["main", "int", "b2b"])
def test_ifs_config_accepts_valid_layers(layer: str) -> None:
    config = IFSConfig(
        host="https://ifs.example.com",
        client_id="client-id",
        client_secret="client-secret",
        layer=layer,
    )
    assert config.layer == layer


def test_ifs_config_rejects_invalid_layer() -> None:
    with pytest.raises(ValueError, match="Invalid IFS layer"):
        IFSConfig(
            host="https://ifs.example.com",
            client_id="client-id",
            client_secret="client-secret",
            layer="invalid",
        )


@patch("fabrictools.integrations.ifs.api.http_request_json")
def test_ifs_request_json_sends_bearer_token(mock_http_request_json: MagicMock) -> None:
    mock_http_request_json.return_value = {"value": []}

    ifs_request_json("my-token", "https://ifs.example.com/main/test")

    mock_http_request_json.assert_called_once_with(
        "GET",
        "https://ifs.example.com/main/test",
        headers={
            "Authorization": "Bearer my-token",
            "Accept": "application/json",
        },
        data=None,
        timeout_seconds=60,
    )


@patch("fabrictools.integrations.ifs.api.ifs_get_json_with_token")
def test_read_ifs_entity_with_token_single_page(mock_get_json: MagicMock) -> None:
    mock_get_json.return_value = {"value": [{"CustomerId": "C1"}]}

    rows = read_ifs_entity_with_token(
        "token-1",
        "https://ifs.example.com",
        "CustomerHandling",
        "CustomerInfoSet",
        layer="main",
        top=5,
    )

    assert rows == [{"CustomerId": "C1"}]
    called_url = mock_get_json.call_args[0][1]
    assert called_url.startswith(
        "https://ifs.example.com/main/ifsapplications/projection/v1/"
        "CustomerHandling.svc/CustomerInfoSet?"
    )
    assert "$top=5" in called_url


@patch("fabrictools.integrations.ifs.api.ifs_get_json_with_token")
def test_read_ifs_entity_with_token_pagination(mock_get_json: MagicMock) -> None:
    mock_get_json.side_effect = [
        {"value": [{"id": 1}, {"id": 2}]},
        {"value": [{"id": 3}]},
    ]

    rows = read_ifs_entity_with_token(
        "token-1",
        "https://ifs.example.com",
        "CustomerHandling",
        "CustomerInfoSet",
        layer="main",
        fetch_all=True,
        page_size=2,
    )

    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert mock_get_json.call_count == 2


@patch("fabrictools.integrations.ifs.api.ifs_get_json_with_token")
def test_read_ifs_entity_with_token_logs_and_returns_empty_on_error(
    mock_get_json: MagicMock,
) -> None:
    from fabrictools.integrations.ifs.errors import IFSError

    mock_get_json.side_effect = IFSError(
        "Insufficient privileges.",
        status_code=403,
        error_code="SE_UNAUTHORIZED",
    )

    rows = read_ifs_entity_with_token(
        "token-1",
        "https://ifs.example.com",
        "CustomerHandling",
        "CustomerInfoSet",
        layer="main",
        top=5,
    )

    assert rows == []


pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("fabrictools_test_ifs_client")
        .getOrCreate()
    )
    yield session
    session.stop()


@patch("fabrictools.integrations.ifs.read.IFSClient")
def test_read_ifs_entity_returns_dataframe(mock_client_cls: MagicMock, spark: SparkSession) -> None:
    from fabrictools.integrations.ifs.read import read_ifs_entity

    mock_client_cls.return_value.get_entity.return_value = [
        {"ActivityId": 1, "Company": "SPH"},
        {"ActivityId": 2, "Company": "SPH"},
    ]

    df = read_ifs_entity(
        _config(),
        "ActivityService",
        "Activities",
        odata_filter="Company eq 'SPH'",
        spark=spark,
    )

    assert df.count() == 2
    assert set(df.columns) == {"ActivityId", "Company"}


def test_parse_ifs_data_accepts_array_and_odata_payload() -> None:
    from fabrictools.integrations.ifs.dataframe import parse_ifs_data

    rows = parse_ifs_data(json.dumps([{"CustomerId": "173", "Name": "ACME"}]))
    assert rows == [{"CustomerId": "173", "Name": "ACME"}]

    rows = parse_ifs_data(json.dumps({"value": [{"CustomerId": "171"}]}))
    assert rows == [{"CustomerId": "171"}]

    assert parse_ifs_data("") == []
    assert parse_ifs_data("   ") == []


def test_ifs_data_to_dataframe_handles_all_null_columns(spark: SparkSession) -> None:
    from fabrictools.integrations.ifs.dataframe import ifs_data_to_dataframe

    ifs_data = json.dumps(
        [
            {
                "CustomerId": "173",
                "Name": "BEDEK-IAI",
                "DefaultDomain": True,
                "AssociationNo": None,
                "CorporateForm": None,
            },
            {
                "CustomerId": "171",
                "Name": "ITA Airways",
                "DefaultDomain": True,
                "AssociationNo": None,
                "CorporateForm": None,
            },
        ]
    )

    df = ifs_data_to_dataframe(ifs_data, spark=spark)

    assert df.count() == 2
    assert "AssociationNo" in df.columns
    assert "CorporateForm" in df.columns
    assert df.filter("CustomerId = '173'").collect()[0]["Name"] == "BEDEK-IAI"


def test_ifs_data_to_dataframe_mixed_int_and_float_columns(spark: SparkSession) -> None:
    from fabrictools.integrations.ifs.dataframe import ifs_data_to_dataframe

    ifs_data = json.dumps(
        [
            {"LineNo": "1", "BaseSaleUnitPrice": 335, "Cost": 108.895},
            {"LineNo": "2", "BaseSaleUnitPrice": 520, "Cost": 159.72666666666666},
        ]
    )

    df = ifs_data_to_dataframe(ifs_data, spark=spark)

    assert df.count() == 2
    row = df.filter("LineNo = '1'").collect()[0]
    assert row["BaseSaleUnitPrice"] == 335.0
    assert row["Cost"] == 108.895


@patch("fabrictools.integrations.ifs.lakehouse.write_lakehouse")
@patch("fabrictools.integrations.ifs.lakehouse.ifs_data_to_dataframe")
def test_write_ifs_data_to_lakehouse(
    mock_ifs_data_to_dataframe: MagicMock,
    mock_write_lakehouse: MagicMock,
    spark: SparkSession,
) -> None:
    from fabrictools.integrations.ifs.lakehouse import write_ifs_data_to_lakehouse

    mock_df = spark.createDataFrame([{"CustomerId": "173"}])
    mock_ifs_data_to_dataframe.return_value = mock_df

    result = write_ifs_data_to_lakehouse(
        json.dumps([{"CustomerId": "173"}]),
        "BronzeLakehouse",
        "Tables/dbo/ifs_customers",
        mode="append",
        spark=spark,
    )

    mock_ifs_data_to_dataframe.assert_called_once_with(
        json.dumps([{"CustomerId": "173"}]),
        spark=spark,
    )
    mock_write_lakehouse.assert_called_once()
    write_args, write_kwargs = mock_write_lakehouse.call_args
    assert write_args[0] is mock_df
    assert write_args[1] == "BronzeLakehouse"
    assert write_args[2] == "Tables/dbo/ifs_customers"
    assert write_kwargs["mode"] == "append"
    assert write_kwargs["spark"] is spark
    assert result is mock_df


@patch("fabrictools.integrations.ifs.lakehouse.write_lakehouses")
@patch("fabrictools.integrations.ifs.lakehouse.ifs_data_to_dataframe")
def test_write_ifs_data_to_lakehouses(
    mock_ifs_data_to_dataframe: MagicMock,
    mock_write_lakehouses: MagicMock,
    spark: SparkSession,
) -> None:
    from fabrictools.integrations.ifs.lakehouse import write_ifs_data_to_lakehouses

    df_one = spark.createDataFrame([{"CustomerId": "173"}])
    df_two = spark.createDataFrame([{"OrderId": "O1"}])
    mock_ifs_data_to_dataframe.side_effect = [df_one, df_two]
    mock_write_lakehouses.return_value = {
        "total_tables": 2,
        "successful_tables": 2,
        "failed_tables": 0,
        "tables": [],
        "failures": [],
    }

    summary = write_ifs_data_to_lakehouses(
        [
            {
                "ifs_data": json.dumps([{"CustomerId": "173"}]),
                "lakehouse_name": "BronzeLakehouse",
                "relative_path": "Tables/dbo/ifs_customers",
            },
            {
                "ifs_data": json.dumps([{"OrderId": "O1"}]),
                "lakehouse_name": "BronzeLakehouse",
                "relative_path": "Tables/dbo/ifs_orders",
                "mode": "append",
            },
        ],
        max_workers=2,
        spark=spark,
    )

    assert mock_ifs_data_to_dataframe.call_count == 2
    mock_write_lakehouses.assert_called_once()
    write_requests = mock_write_lakehouses.call_args[0][0]
    assert len(write_requests) == 2
    assert write_requests[0]["df"] is df_one
    assert write_requests[0]["lakehouse_name"] == "BronzeLakehouse"
    assert write_requests[0]["relative_path"] == "Tables/dbo/ifs_customers"
    assert write_requests[1]["df"] is df_two
    assert write_requests[1]["mode"] == "append"
    assert mock_write_lakehouses.call_args[1]["max_workers"] == 2
    assert mock_write_lakehouses.call_args[1]["spark"] is spark
    assert summary["successful_tables"] == 2


def test_write_ifs_data_to_lakehouses_requires_ifs_data() -> None:
    from fabrictools.integrations.ifs.lakehouse import write_ifs_data_to_lakehouses

    with pytest.raises(ValueError, match="missing required key 'ifs_data'"):
        write_ifs_data_to_lakehouses(
            [{"lakehouse_name": "BronzeLakehouse", "relative_path": "Tables/dbo/ifs_customers"}]
        )


def test_write_ifs_data_to_lakehouses_empty_requests() -> None:
    from fabrictools.integrations.ifs.lakehouse import write_ifs_data_to_lakehouses

    summary = write_ifs_data_to_lakehouses([])

    assert summary == {
        "total_tables": 0,
        "successful_tables": 0,
        "failed_tables": 0,
        "tables": [],
        "failures": [],
    }
