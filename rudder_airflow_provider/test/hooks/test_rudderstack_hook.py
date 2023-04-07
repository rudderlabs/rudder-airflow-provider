import unittest
from unittest import mock
from airflow.exceptions import AirflowException

from airflow.models.connection import Connection
from requests.models import Response
from rudder_airflow_provider.hooks.rudderstack import RudderstackHook


class RudderstackHookTest(unittest.TestCase):

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    def test_get_access_token(self, mocked_http: mock.Mock):
        rudder_connection = Connection(password='some-password')
        mocked_http.return_value = rudder_connection
        hook = RudderstackHook('rudderstack_connection', '12345')
        self.assertEqual(hook.get_access_token(), 'some-password')

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.run')
    def test_trigger_sync(self, mock_run: mock.Mock, mock_connection: mock.Mock):
        source_id = 'some-source-id'
        access_token = 'some-password'
        hook = RudderstackHook('rudderstack_connection', source_id)
        mock_connection.return_value = Connection(password=access_token)
        sync_endpoint = f"/v2/sources/{source_id}/start"
        start_resp = Response()
        start_resp.status_code = 204
        mock_run.return_value = start_resp
        hook.trigger_sync()
        expected_headers = {
                 'authorization': f"Bearer {access_token}",
                 'Content-Type': 'application/json'
                 }
        mock_run.assert_called_once_with(endpoint=sync_endpoint, headers=expected_headers, extra_options={'check_response': False})
    
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.run')
    def test_trigger_sync_conflict_status(self, mock_run: mock.Mock, mock_connection: mock.Mock):
        source_id = 'some-source-id'
        access_token = 'some-password'
        hook = RudderstackHook('rudderstack_connection', source_id)
        mock_connection.return_value = Connection(password=access_token)
        sync_endpoint = f"/v2/sources/{source_id}/start"
        start_resp = Response()
        start_resp.status_code = 409
        mock_run.return_value = start_resp
        hook.trigger_sync()
        expected_headers = {
                 'authorization': f"Bearer {access_token}",
                 'Content-Type': 'application/json'
                 }
        mock_run.assert_called_once_with(endpoint=sync_endpoint, headers=expected_headers, extra_options={'check_response': False})
    
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.run')
    def test_trigger_sync_error_status(self, mock_run: mock.Mock, mock_connection: mock.Mock):
        source_id = 'some-source-id'
        access_token = 'some-password'
        hook = RudderstackHook('rudderstack_connection', source_id)
        mock_connection.return_value = Connection(password=access_token)
        sync_endpoint = f"/v2/sources/{source_id}/start"
        start_resp = Response()
        start_resp.status_code = 500
        mock_run.return_value = start_resp
        self.assertRaises(AirflowException, hook.trigger_sync)
        expected_headers = {
                 'authorization': f"Bearer {access_token}",
                 'Content-Type': 'application/json'
                 }
        mock_run.assert_called_once_with(endpoint=sync_endpoint, headers=expected_headers, extra_options={'check_response': False})

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.run')
    def test_triger_sync_exception(self, mock_run: mock.Mock, mock_connection: mock.Mock):
        source_id = 'some-source-id'
        access_token = 'some-password'
        mock_connection.return_value = Connection(password=access_token)
        mock_run.side_effect = AirflowException()
        hook = RudderstackHook('rudderstack_connection', source_id)
        self.assertRaises(AirflowException, hook.trigger_sync)

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.run')
    def test_poll_status(self, mock_run: mock.Mock, mock_connection: mock.Mock):
        source_id = 'some-source-id'
        access_token = 'some-password'
        status_endpoint = f"/v2/sources/{source_id}/status"
        finished_status_response = Response()
        finished_status_response.status_code = 200
        running_resp = {
            'id': 'cgnrus5fsq3vbs6li0hg',
            'job_id': '27abc3Nh4NpaIRDalQAtHdboI5R',
            'started_at': '2023-04-07T07:00:00.002103Z'}
        finished_resp = {
            'id': 'cgnrus5fsq3vbs6li0hg',
            'job_id': '27abc3Nh4NpaIRDalQAtHdboI5R',
            'started_at': '2023-04-07T07:00:00.002103Z',
            'finished_at': '2023-04-07T07:10:14.826631Z'}

        finished_status_response.json = mock.MagicMock(side_effect=[running_resp, finished_resp])
        mock_run.return_value = finished_status_response
        mock_connection.return_value = Connection(password=access_token)
        hook = RudderstackHook('rudderstack_connection', source_id)
        hook.poll_for_status()
        expected_headers = {
                 'authorization': f"Bearer {access_token}",
                 'Content-Type': 'application/json'
                 }
        mock_run.assert_called_with(endpoint=status_endpoint, headers=expected_headers)
        self.assertEqual(mock_run.call_count, 2)

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.run')
    def test_poll_status_failure(self, mock_run: mock.Mock, mock_connection: mock.Mock):
        source_id = 'some-source-id'
        access_token = 'some-password'
        status_endpoint = f"/v2/sources/{source_id}/status"
        finished_status_response = Response()
        finished_status_response.status_code = 200
        finished_status_response.json = mock.MagicMock(return_value={
            'id': 'cgnrus5fsq3vbs6li0hg',
            'job_id': '27abc3Nh4NpaIRDalQAtHdboI5R',
            'started_at': '2023-04-07T07:00:00.002103Z',
            'finished_at': '2023-04-07T07:10:14.826631Z',
            'error': 'some-eror'})
        mock_run.return_value = finished_status_response
        mock_connection.return_value = Connection(password=access_token)
        hook = RudderstackHook('rudderstack_connection', source_id)
        self.assertRaises(AirflowException, hook.poll_for_status)
        expected_headers = {
                 'authorization': f"Bearer {access_token}",
                 'Content-Type': 'application/json'
                 }
        mock_run.assert_called_once_with(endpoint=status_endpoint, headers=expected_headers)



if __name__ == '__main__':
    unittest.main()
