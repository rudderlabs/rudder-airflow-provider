import unittest
from unittest import mock

from rudder_airflow_provider.operators.rudderstack import RudderstackOperator


class TestRudderstackOperator(unittest.TestCase):

    @mock.patch('rudder_airflow_provider.operators.rudderstack.RudderstackHook.poll_for_status')
    @mock.patch('rudder_airflow_provider.operators.rudderstack.RudderstackHook.trigger_sync')
    def test_operator_trigger_sync_without_wait(self, mock_hook_sync: mock.Mock, 
        mock_poll_status: mock.Mock):
        mock_hook_sync.return_value = 'some-run-id'
        operator = RudderstackOperator(source_id='some-source-id', 
            wait_for_completion=False, task_id='some-task-id')
        operator.execute(context=None)
        mock_hook_sync.assert_called_once()
        mock_poll_status.assert_not_called()

    @mock.patch('rudder_airflow_provider.operators.rudderstack.RudderstackHook.poll_for_status')
    @mock.patch('rudder_airflow_provider.operators.rudderstack.RudderstackHook.trigger_sync')
    def test_operator_trigger_sync_with_wait(self, mock_hook_sync: mock.Mock, 
        mock_poll_status: mock.Mock):
        mock_hook_sync.return_value = 'some-run-id'
        mock_poll_status.return_value = None
        operator = RudderstackOperator(source_id='some-source-id',
            wait_for_completion=True, task_id='some-task-id')
        operator.execute(context=None)
        mock_hook_sync.assert_called_once()
        mock_poll_status.assert_called_once()
    
    # checks if poll_for_status is not called if run_id is None (possible if sync is already running)
    @mock.patch('rudder_airflow_provider.operators.rudderstack.RudderstackHook.poll_for_status')
    @mock.patch('rudder_airflow_provider.operators.rudderstack.RudderstackHook.trigger_sync')
    def test_operator_no_polling_if_run_not_started(self, mock_hook_sync: mock.Mock, mock_poll_status: mock.Mock):
        operator = RudderstackOperator(source_id='some-source-id',
            wait_for_completion=True, task_id='some-task-id')
        mock_hook_sync.return_value = None
        operator.execute(context=None)
        mock_hook_sync.assert_called_once()
        mock_poll_status.assert_not_called()

if __name__ == '__main__':
    unittest.main()