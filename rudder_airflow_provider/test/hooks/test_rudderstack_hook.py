import pytest
from unittest.mock import patch, MagicMock, ANY
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from requests.exceptions import RequestException
from requests.exceptions import Timeout
from rudder_airflow_provider.hooks.rudderstack import (
    BaseRudderStackHook,
    RudderStackETLHook,
    RudderStackRETLHook,
    RETLSyncStatus,
    RudderStackProfilesHook,
    ProfilesRunStatus,
    ETLRunStatus
)

# Mocking constants for testing
TEST_AIRFLOW_CONN_ID = "airflow_conn_id"
TEST_RETL_CONN_ID = "test_retl_conn_id"
TEST_PROFILE_ID = "test_profile_id"
TEST_RETL_SYNC_RUN_ID = "test_retl_sync_id"
TEST_PROFILE_RUN_ID = "test_profile_run_id"
TEST_ETL_SOURCE_ID = "test_etl_src_id"
TEST_ETL_SYNC_RUN_ID = "test_etl_sync_id"
TEST_ACCESS_TOKEN = "test_access_token"
TEST_BASE_URL = "http://test.rudderstack.api"


# Mocking connection and responses
@pytest.fixture
def airflow_connection():
    return Connection(
        conn_id=TEST_AIRFLOW_CONN_ID, host=TEST_BASE_URL, password=TEST_ACCESS_TOKEN
    )


# BaseRudderStackHook tests
@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
def test_get_access_token(mock_connection, airflow_connection):
    basehook = BaseRudderStackHook(TEST_AIRFLOW_CONN_ID)
    mock_connection.return_value = airflow_connection
    assert basehook._get_access_token() == TEST_ACCESS_TOKEN


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
def test_get_api_base_url(mock_connection, airflow_connection):
    mock_connection.return_value = airflow_connection
    basehook = BaseRudderStackHook(TEST_AIRFLOW_CONN_ID)
    assert basehook._get_api_base_url() == TEST_BASE_URL


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
def test_get_request_headers(mock_connection, airflow_connection):
    mock_connection.return_value = airflow_connection
    basehook = BaseRudderStackHook(TEST_AIRFLOW_CONN_ID)
    headers = basehook._get_request_headers()
    assert headers["authorization"] == f"Bearer {TEST_ACCESS_TOKEN}"
    assert headers["Content-Type"] == "application/json"


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_make_request_success(mock_request, mock_connection, airflow_connection):
    mock_request.return_value = MagicMock(
        status_code=200, json=lambda: {"result": "success"}
    )
    mock_connection.return_value = airflow_connection
    basehook = BaseRudderStackHook(TEST_AIRFLOW_CONN_ID)
    result = basehook.make_request("/endpoint", method="GET")
    assert result == {"result": "success"}
    mock_request.assert_called_once_with(
        method="GET",
        url=TEST_BASE_URL + "/endpoint",
        headers=ANY,
        timeout=30,
    )


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_make_request_failure(mock_request, mock_connection, airflow_connection):
    mock_request.side_effect = RequestException("Request failed")
    mock_connection.return_value = airflow_connection
    basehook = BaseRudderStackHook(TEST_AIRFLOW_CONN_ID)

    with pytest.raises(AirflowException, match="Exceeded max number of retries"):
        basehook.make_request("/endpoint")
    assert mock_request.call_count == 4


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_make_request_success_after_retry(
    mock_request, mock_connection, airflow_connection
):
    mock_request.side_effect = [
        Timeout(),
        Timeout(),
        MagicMock(status_code=200, json=lambda: {"result": "success"}),
    ]
    mock_connection.return_value = airflow_connection
    basehook = BaseRudderStackHook(TEST_AIRFLOW_CONN_ID)
    response = basehook.make_request(endpoint="/test-endpoint", method="GET")
    assert response == {"result": "success"}
    assert mock_request.call_count == 3


# RudderStackRETLHook tests
@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_start_sync(mock_request, mock_connection, airflow_connection):
    mock_request.return_value = MagicMock(
        status_code=200, json=lambda: {"syncId": TEST_RETL_SYNC_RUN_ID}
    )
    mock_connection.return_value = airflow_connection
    retl_hook = RudderStackRETLHook(TEST_AIRFLOW_CONN_ID)
    sync_id = retl_hook.start_sync(TEST_RETL_CONN_ID)
    assert sync_id == TEST_RETL_SYNC_RUN_ID

    mock_request.assert_called_once_with(
        method="POST",
        url=f"{TEST_BASE_URL}/v2/retl-connections/{TEST_RETL_CONN_ID}/start",
        headers=ANY,
        timeout=30,
        json={},
    )


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
def test_start_sync_invalid_parameters(mock_connection, airflow_connection):
    mock_connection.return_value = airflow_connection
    retl_hook = RudderStackRETLHook(TEST_AIRFLOW_CONN_ID)
    with pytest.raises(AirflowException, match="Invalid sync type: invalid_sync_type"):
        retl_hook.start_sync(TEST_RETL_CONN_ID, "invalid_sync_type")

    with pytest.raises(AirflowException, match="retl_connection_id is required"):
        retl_hook.start_sync("")


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_sync_success(mock_request, mock_connection, airflow_connection):
    mock_request.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_RETL_SYNC_RUN_ID,
                "status": RETLSyncStatus.RUNNING,
            },
        ),
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_RETL_SYNC_RUN_ID,
                "status": RETLSyncStatus.SUCCEEDED,
            },
        ),
    ]
    mock_connection.return_value = airflow_connection
    retl_hook = RudderStackRETLHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1
    )
    result = retl_hook.poll_sync(TEST_RETL_CONN_ID, TEST_RETL_SYNC_RUN_ID)
    assert mock_request.call_count == 2
    mock_request.assert_called_with(
        method="GET",
        url=f"{TEST_BASE_URL}/v2/retl-connections/{TEST_RETL_CONN_ID}/syncs/{TEST_RETL_SYNC_RUN_ID}",
        headers=ANY,
        timeout=30,
    )
    assert result == {"id": TEST_RETL_SYNC_RUN_ID, "status": RETLSyncStatus.SUCCEEDED}


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_sync_timeout(mock_request, mock_connection, airflow_connection):
    mock_request.return_value = MagicMock(
        status_code=200,
        json=lambda: {"id": TEST_RETL_SYNC_RUN_ID, "status": RETLSyncStatus.RUNNING},
    )
    mock_connection.return_value = airflow_connection
    retl_hook = RudderStackRETLHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1, poll_timeout=0.3
    )
    with pytest.raises(
        AirflowException,
        match="Polling for syncId: test_retl_sync_id for retl connection: test_retl_conn_id timed out",
    ):
        retl_hook.poll_sync(TEST_RETL_CONN_ID, TEST_RETL_SYNC_RUN_ID)
    assert mock_request.call_count <= 4


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_retl_sync_failed(mock_request, mock_connection, airflow_connection):
    """Test that RETL sync properly raises exception when status is 'failed'"""
    mock_request.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_RETL_SYNC_RUN_ID,
                "status": RETLSyncStatus.RUNNING,
            },
        ),
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_RETL_SYNC_RUN_ID,
                "status": RETLSyncStatus.FAILED,
                "error": "Destination connection failed",
            },
        ),
    ]
    mock_connection.return_value = airflow_connection
    retl_hook = RudderStackRETLHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1
    )
    with pytest.raises(
        AirflowException,
        match="Sync for retl connection: test_retl_conn_id, syncId: test_retl_sync_id failed with error: Destination connection failed",
    ):
        retl_hook.poll_sync(TEST_RETL_CONN_ID, TEST_RETL_SYNC_RUN_ID)
    assert mock_request.call_count == 2


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_retl_sync_failed_no_error_message(mock_request, mock_connection, airflow_connection):
    """Test that RETL sync raises exception even when error field is None"""
    mock_request.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_RETL_SYNC_RUN_ID,
                "status": RETLSyncStatus.RUNNING,
            },
        ),
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_RETL_SYNC_RUN_ID,
                "status": RETLSyncStatus.FAILED,
                "error": None,
            },
        ),
    ]
    mock_connection.return_value = airflow_connection
    retl_hook = RudderStackRETLHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1
    )
    with pytest.raises(
        AirflowException,
        match="Sync for retl connection: test_retl_conn_id, syncId: test_retl_sync_id failed with error: None",
    ):
        retl_hook.poll_sync(TEST_RETL_CONN_ID, TEST_RETL_SYNC_RUN_ID)
    assert mock_request.call_count == 2


# RudderStackProfilesHook tests
@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_start_profile_run(mock_request, mock_connection, airflow_connection):
    mock_request.return_value = MagicMock(
        status_code=200, json=lambda: {"runId": TEST_PROFILE_RUN_ID}
    )
    mock_connection.return_value = airflow_connection
    profiles_hook = RudderStackProfilesHook(TEST_AIRFLOW_CONN_ID)
    run_id = profiles_hook.start_profile_run(TEST_PROFILE_ID)
    assert run_id == TEST_PROFILE_RUN_ID
    mock_request.assert_called_once_with(
        method="POST",
        url=f"{TEST_BASE_URL}/v2/sources/{TEST_PROFILE_ID}/start",
        headers=ANY,
        timeout=30,
    )

@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_start_profile_run_parameters(mock_request, mock_connection, airflow_connection):
    mock_request.return_value = MagicMock(
        status_code=200, json=lambda: {"runId": TEST_PROFILE_RUN_ID}
    )
    mock_connection.return_value = airflow_connection
    profiles_hook = RudderStackProfilesHook(TEST_AIRFLOW_CONN_ID)
    run_id = profiles_hook.start_profile_run(TEST_PROFILE_ID, ["--rebase_incremental"])
    assert run_id == TEST_PROFILE_RUN_ID
    mock_request.assert_called_once_with(
        method="POST",
        url=f"{TEST_BASE_URL}/v2/sources/{TEST_PROFILE_ID}/start",
        json={'parameters': ['--rebase_incremental']},
        headers=ANY,
        timeout=30,
    )

@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
def test_start_profile_run_invalid_parameters(mock_connection, airflow_connection):
    mock_connection.return_value = airflow_connection
    profiles_hook = RudderStackProfilesHook(TEST_PROFILE_ID)
    with pytest.raises(
        AirflowException, match="profile_id is required to start a profile run"
    ):
        profiles_hook.start_profile_run("")


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_profile_run_success(mock_request, mock_connection, airflow_connection):
    mock_request.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_PROFILE_RUN_ID,
                "status": ProfilesRunStatus.RUNNING,
            },
        ),
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_PROFILE_RUN_ID,
                "status": ProfilesRunStatus.FINISHED,
            },
        ),
    ]
    mock_connection.return_value = airflow_connection
    profiles_hook = RudderStackProfilesHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1
    )
    result = profiles_hook.poll_profile_run(TEST_PROFILE_ID, TEST_PROFILE_RUN_ID)
    assert mock_request.call_count == 2
    mock_request.assert_called_with(
        method="GET",
        url=f"{TEST_BASE_URL}/v2/sources/{TEST_PROFILE_ID}/runs/{TEST_PROFILE_RUN_ID}/status",
        headers=ANY,
        timeout=30,
    )
    assert result == {"id": TEST_PROFILE_RUN_ID, "status": ProfilesRunStatus.FINISHED}


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_profile_run_timeout(mock_request, mock_connection, airflow_connection):
    mock_request.return_value = MagicMock(
        status_code=200,
        json=lambda: {"id": TEST_PROFILE_RUN_ID, "status": ProfilesRunStatus.RUNNING},
    )
    mock_connection.return_value = airflow_connection
    profiles_hook = RudderStackProfilesHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1, poll_timeout=0.3
    )
    with pytest.raises(
        AirflowException,
        match="Polling for runId: test_profile_run_id for profile: test_profile_id timed out",
    ):
        profiles_hook.poll_profile_run(TEST_PROFILE_ID, TEST_PROFILE_RUN_ID)
    assert mock_request.call_count <= 4


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_profile_run_finished_with_error(mock_request, mock_connection, airflow_connection):
    """Test PRO-4785: Profile run returns 'finished' status but contains error field"""
    mock_request.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_PROFILE_RUN_ID,
                "status": ProfilesRunStatus.RUNNING,
            },
        ),
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_PROFILE_RUN_ID,
                "status": ProfilesRunStatus.FINISHED,
                "error": "Configuration error: Invalid warehouse credentials",
            },
        ),
    ]
    mock_connection.return_value = airflow_connection
    profiles_hook = RudderStackProfilesHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1
    )
    with pytest.raises(
        AirflowException,
        match="Profile run for profile: test_profile_id, runId: test_profile_run_id failed with error: Configuration error: Invalid warehouse credentials",
    ):
        profiles_hook.poll_profile_run(TEST_PROFILE_ID, TEST_PROFILE_RUN_ID)
    assert mock_request.call_count == 2


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_profile_run_finished_no_error(mock_request, mock_connection, airflow_connection):
    """Test that profile run succeeds when status is 'finished' with no error field"""
    mock_request.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_PROFILE_RUN_ID,
                "status": ProfilesRunStatus.RUNNING,
            },
        ),
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_PROFILE_RUN_ID,
                "status": ProfilesRunStatus.FINISHED,
                "error": None,
            },
        ),
    ]
    mock_connection.return_value = airflow_connection
    profiles_hook = RudderStackProfilesHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1
    )
    result = profiles_hook.poll_profile_run(TEST_PROFILE_ID, TEST_PROFILE_RUN_ID)
    assert mock_request.call_count == 2
    assert result["status"] == ProfilesRunStatus.FINISHED
    assert result["error"] is None


#RudderStackETLHook tests
@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_etl_start_sync(mock_request, mock_connection, airflow_connection):
    mock_request.return_value = MagicMock(status_code=200, json=lambda: {"runId": TEST_ETL_SYNC_RUN_ID})
    mock_connection.return_value =airflow_connection
    
    etl_hook = RudderStackETLHook(TEST_AIRFLOW_CONN_ID)
    run_id = etl_hook.start_sync(TEST_ETL_SOURCE_ID)
    assert run_id == TEST_ETL_SYNC_RUN_ID
    
    mock_request.assert_called_once_with(
        method="POST",
        url=f"{TEST_BASE_URL}/v2/sources/{TEST_ETL_SOURCE_ID}/start",
        headers=ANY,
        timeout=30,
    )

@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
def test_etl_start_sync_invalid_parameters(mock_connection, airflow_connection):
    mock_connection.return_value = airflow_connection
    etl_hook = RudderStackETLHook(TEST_AIRFLOW_CONN_ID)
    with pytest.raises(
        AirflowException, match="source_id is required to start an ETL sync"
    ):
        etl_hook.start_sync("")



@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
def test_etl_poll_sync_invalid_parameters(mock_connection, airflow_connection):
    mock_connection.return_value = airflow_connection
    etl_hook = RudderStackETLHook(TEST_AIRFLOW_CONN_ID)
    with pytest.raises(
        AirflowException, match="source_id is required to start a sync run"
    ):
        etl_hook.poll_sync("", "")
    
    with pytest.raises(
        AirflowException, match="run_id is required to poll status of sync run"
    ):
        etl_hook.poll_sync(TEST_ETL_SOURCE_ID, "")


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_etl_sync_success(mock_request, mock_connection, airflow_connection):
    mock_request.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_ETL_SYNC_RUN_ID,
                "status": ETLRunStatus.RUNNING,
            },
        ),
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_ETL_SYNC_RUN_ID,
                "status": ETLRunStatus.FINISHED,
            },
        ),
    ]
    mock_connection.return_value = airflow_connection
    etl_hook = RudderStackETLHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1
    )
    result = etl_hook.poll_sync(TEST_ETL_SOURCE_ID, TEST_ETL_SYNC_RUN_ID)
    assert mock_request.call_count == 2
    mock_request.assert_called_with(
        method="GET",
        url=f"{TEST_BASE_URL}/v2/sources/{TEST_ETL_SOURCE_ID}/runs/{TEST_ETL_SYNC_RUN_ID}/status",
        headers=ANY,
        timeout=30,
    )
    assert result == {"id": TEST_ETL_SYNC_RUN_ID, "status": ETLRunStatus.FINISHED}


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_etl_sync_timeout(mock_request, mock_connection, airflow_connection):
    mock_request.return_value = MagicMock(
        status_code=200,
        json=lambda: {"id": TEST_ETL_SYNC_RUN_ID, "status": ETLRunStatus.RUNNING},
    )
    mock_connection.return_value = airflow_connection
    etl_hook = RudderStackETLHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1, poll_timeout=0.3
    )
    with pytest.raises(
        AirflowException,
        match=f"Polling for runId: {TEST_ETL_SYNC_RUN_ID} for source: {TEST_ETL_SOURCE_ID} timed out",
    ):
        etl_hook.poll_sync(TEST_ETL_SOURCE_ID, TEST_ETL_SYNC_RUN_ID)
    assert mock_request.call_count <= 4


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_etl_sync_finished_with_error(mock_request, mock_connection, airflow_connection):
    """Test that ETL sync returns 'finished' status but contains error field"""
    mock_request.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_ETL_SYNC_RUN_ID,
                "status": ETLRunStatus.RUNNING,
            },
        ),
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_ETL_SYNC_RUN_ID,
                "status": ETLRunStatus.FINISHED,
                "error": "Source authentication failed",
            },
        ),
    ]
    mock_connection.return_value = airflow_connection
    etl_hook = RudderStackETLHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1
    )
    with pytest.raises(
        AirflowException,
        match="Sync run for source: test_etl_src_id, runId: test_etl_sync_id failed with error: Source authentication failed",
    ):
        etl_hook.poll_sync(TEST_ETL_SOURCE_ID, TEST_ETL_SYNC_RUN_ID)
    assert mock_request.call_count == 2


@patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
@patch("requests.request")
def test_poll_etl_sync_finished_no_error(mock_request, mock_connection, airflow_connection):
    """Test that ETL sync succeeds when status is 'finished' with no error field"""
    mock_request.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_ETL_SYNC_RUN_ID,
                "status": ETLRunStatus.RUNNING,
            },
        ),
        MagicMock(
            status_code=200,
            json=lambda: {
                "id": TEST_ETL_SYNC_RUN_ID,
                "status": ETLRunStatus.FINISHED,
                "error": None,
            },
        ),
    ]
    mock_connection.return_value = airflow_connection
    etl_hook = RudderStackETLHook(
        connection_id=TEST_AIRFLOW_CONN_ID, poll_interval=0.1
    )
    result = etl_hook.poll_sync(TEST_ETL_SOURCE_ID, TEST_ETL_SYNC_RUN_ID)
    assert mock_request.call_count == 2
    assert result["status"] == ETLRunStatus.FINISHED
    assert result["error"] is None


if __name__ == "__main__":
    pytest.main()

