import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException
from rudder_airflow_provider.operators.rudderstack import (
    RudderstackRETLOperator,
    RudderstackProfilesOperator
)
from rudder_airflow_provider.hooks.rudderstack import (
    RETLSyncStatus,
    ProfilesRunStatus
)

# Constants for test cases
TEST_RETL_CONNECTION_ID = "test_retl_connection"
TEST_PROFILE_ID = "test_profile_id"
TEST_SYNC_ID = "test_sync_id"
TEST_PROFILES_RUN_ID = "test_run_id"


# Test RudderstackRETLOperator
@patch('rudder_airflow_provider.hooks.rudderstack.RudderStackRETLHook.poll_sync')
@patch('rudder_airflow_provider.hooks.rudderstack.RudderStackRETLHook.start_sync')
def test_retl_operator_execute_without_wait(mock_start_sync, mock_poll_sync):
    mock_start_sync.return_value = TEST_SYNC_ID
    retl_operator = RudderstackRETLOperator(retl_connection_id=TEST_RETL_CONNECTION_ID,
                                       wait_for_completion=False,
                                       task_id='some-task-id')
    retl_operator.execute(context=None)
    mock_start_sync.assert_called_once_with(TEST_RETL_CONNECTION_ID, None)
    mock_poll_sync.assert_not_called()


@patch('rudder_airflow_provider.hooks.rudderstack.RudderStackRETLHook.poll_sync')
@patch('rudder_airflow_provider.hooks.rudderstack.RudderStackRETLHook.start_sync')
def test_retl_operator_execute_with_wait(mock_start_sync, mock_poll_sync):
    mock_start_sync.return_value = TEST_SYNC_ID
    mock_poll_sync.return_value = [
        {
            "id": TEST_SYNC_ID,
            "job_id": TEST_RETL_CONNECTION_ID,
            "status": RETLSyncStatus.SUCCEEDED,
            }
    ]
    retl_operator = RudderstackRETLOperator(retl_connection_id=TEST_RETL_CONNECTION_ID,
                                       task_id='some-task-id')
    retl_operator.execute(context=None)

    mock_start_sync.assert_called_once_with(TEST_RETL_CONNECTION_ID, None)
    mock_poll_sync.assert_called_once_with(TEST_RETL_CONNECTION_ID, TEST_SYNC_ID)


# Test RudderstackProfilesOperator
@patch('rudder_airflow_provider.hooks.rudderstack.RudderStackProfilesHook.poll_profile_run')
@patch('rudder_airflow_provider.hooks.rudderstack.RudderStackProfilesHook.start_profile_run')
def test_profiles_operator_execute_without_wait(mock_profile_run, mock_poll_profile_run):
    mock_profile_run.return_value = TEST_PROFILES_RUN_ID
    profiles_operator = RudderstackProfilesOperator(profile_id=TEST_PROFILE_ID,
                                       wait_for_completion=False,
                                       task_id='some-task-id')
    profiles_operator.execute(context=None)
    mock_profile_run.assert_called_once_with(TEST_PROFILE_ID)
    mock_poll_profile_run.assert_not_called()


@patch('rudder_airflow_provider.hooks.rudderstack.RudderStackProfilesHook.poll_profile_run')
@patch('rudder_airflow_provider.hooks.rudderstack.RudderStackProfilesHook.start_profile_run')
def test_profiles_operator_execute_with_wait(mock_profile_run, mock_poll_profile_run):
    mock_profile_run.return_value = TEST_PROFILES_RUN_ID
    mock_poll_profile_run.return_value = [
        {
            "id": TEST_PROFILES_RUN_ID,
            "job_id": TEST_PROFILE_ID,
            "status": ProfilesRunStatus.FINISHED,
            }
    ]
    profiles_operator = RudderstackProfilesOperator(profile_id=TEST_PROFILE_ID,
                                       task_id='some-task-id')
    profiles_operator.execute(context=None)

    mock_profile_run.assert_called_once_with(TEST_PROFILE_ID)
    mock_poll_profile_run.assert_called_once_with(TEST_PROFILE_ID, TEST_PROFILES_RUN_ID)

if __name__ == "__main__":
    pytest.main()