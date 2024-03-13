import unittest
from unittest import mock
from airflow.exceptions import AirflowException

from airflow.models.connection import Connection
from requests.models import Response
from rudder_airflow_provider.hooks.rudderstack import STATUS_POLL_INTERVAL, RudderstackHook


class RudderstackHookTest(unittest.TestCase):

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    def test_get_access_token(self, mocked_http: mock.Mock):
        rudder_connection = Connection(password='some-password')
        mocked_http.return_value = rudder_connection
        hook = RudderstackHook('rudderstack_connection')
        self.assertEqual(hook.get_access_token(), 'some-password')

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.run')
    def test_trigger_sync(self, mock_run: mock.Mock, mock_connection: mock.Mock):
        source_id = 'some-source-id'
        access_token = 'some-password'
        hook = RudderstackHook('rudderstack_connection')
        mock_connection.return_value = Connection(password=access_token)
        sync_endpoint = f"/v2/sources/{source_id}/start"
        start_resp = Response()
        start_resp.json = mock.MagicMock(return_value={'runId': 'some-run-id'})
        start_resp.status_code = 204
        mock_run.return_value = start_resp
        run_id = hook.trigger_sync(source_id)
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
        hook = RudderstackHook('rudderstack_connection')
        mock_connection.return_value = Connection(password=access_token)
        sync_endpoint = f"/v2/sources/{source_id}/start"
        start_resp = Response()
        start_resp.status_code = 409
        mock_run.return_value = start_resp
        run_id = hook.trigger_sync(source_id)
        self.assertIsNone(run_id)
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
        hook = RudderstackHook('rudderstack_connection')
        mock_connection.return_value = Connection(password=access_token)
        sync_endpoint = f"/v2/sources/{source_id}/start"
        start_resp = Response()
        start_resp.status_code = 500
        mock_run.return_value = start_resp
        self.assertRaises(AirflowException, hook.trigger_sync, source_id)
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
        hook = RudderstackHook('rudderstack_connection')
        self.assertRaises(AirflowException, hook.trigger_sync, source_id)

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.run')
    def test_poll_status(self, mock_run: mock.Mock, mock_connection: mock.Mock):
        source_id = 'some-source-id'
        run_id = 'some-run-id'
        access_token = 'some-password'
        status_endpoint = f"/v2/sources/{source_id}/runs/{run_id}/status"
        finished_status_response = Response()
        finished_status_response.status_code = 200
        finished_status_response.json = mock.MagicMock(return_value={'status': 'finished'})
        mock_run.return_value = finished_status_response
        mock_connection.return_value = Connection(password=access_token)
        hook = RudderstackHook('rudderstack_connection')
        hook.poll_for_status(source_id, run_id)
        expected_headers = {
                 'authorization': f"Bearer {access_token}",
                 'Content-Type': 'application/json'
                 }
        mock_run.assert_called_once_with(endpoint=status_endpoint, headers=expected_headers)

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.run')
    def test_poll_status_failure(self, mock_run: mock.Mock, mock_connection: mock.Mock):
        source_id = 'some-source-id'
        run_id = 'some-run-id'
        access_token = 'some-password'
        status_endpoint = f"/v2/sources/{source_id}/runs/{run_id}/status"
        finished_status_response = Response()
        finished_status_response.status_code = 200
        finished_status_response.json = mock.MagicMock(
            return_value={'status': 'finished', 'error': 'some-eror'})
        mock_run.return_value = finished_status_response
        mock_connection.return_value = Connection(password=access_token)
        hook = RudderstackHook('rudderstack_connection')
        self.assertRaises(AirflowException, hook.poll_for_status, source_id, run_id)
        expected_headers = {
                 'authorization': f"Bearer {access_token}",
                 'Content-Type': 'application/json'
                 }
        mock_run.assert_called_once_with(endpoint=status_endpoint, headers=expected_headers)

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.requests.post')
    def test_retl_trigger_sync(self, mock_post: mock.Mock, mock_connection: mock.Mock):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'syncId': 'some-sync-id'}
        mock_connection.side_effect = [Connection(password='some-password', host='https://some-url.com'), Connection(password='some-password')]
        retl_connection_id = 'some-connection-id'
        sync_type = 'full'
        base_url = 'https://some-url.com'
        retl_sync_endpoint = f"/v2/retl-connections/{retl_connection_id}/start"
        hook = RudderstackHook('rudderstack_connection')
        sync_id = hook.trigger_retl_sync(retl_connection_id, sync_type)
        self.assertEqual(sync_id, 'some-sync-id')
        mock_post.assert_called_once_with(f"{base_url}{retl_sync_endpoint}", json={'syncType': sync_type},
                                          headers={'authorization' : f"Bearer some-password", 'Content-Type': 'application/json'})
        mock_connection.assert_called_with('rudderstack_connection')

    # test 409 retl trigger sync and expect AirflowException
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.requests.post')
    def test_retl_trigger_sync_conflict(self, mock_post: mock.Mock, mock_connection: mock.Mock):
        mock_post.return_value.status_code = 409
        mock_connection.side_effect = [Connection(password='some-password', host='https://some-url.com'), Connection(password='some-password')]
        retl_connection_id = 'some-connection-id'
        sync_type = 'full'
        base_url = 'https://some-url.com'
        retl_sync_endpoint = f"/v2/retl-connections/{retl_connection_id}/start"
        hook = RudderstackHook('rudderstack_connection')
        self.assertRaises(AirflowException, hook.trigger_retl_sync, retl_connection_id, sync_type)
        mock_post.assert_called_once_with(f"{base_url}{retl_sync_endpoint}", json={'syncType': sync_type},
                                          headers={'authorization' : f"Bearer some-password", 'Content-Type': 'application/json'})
        mock_connection.assert_called_with('rudderstack_connection')

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.requests.get')
    def test_poll_for_retl_sync_status(self, mock_get: mock.Mock, mock_connection: mock.Mock):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {'status': 'succeeded'}
        mock_connection.side_effect = [Connection(password='some-password', host='https://some-url.com'), Connection(password='some-password')]
        retl_connection_id = 'some-connection-id'
        sync_id = 'some-sync-id'
        base_url = 'https://some-url.com'
        retl_sync_status_endpoint = f"/v2/retl-connections/{retl_connection_id}/syncs/{sync_id}"
        hook = RudderstackHook('rudderstack_connection')
        hook.poll_retl_sync_status(retl_connection_id, sync_id)
        mock_get.assert_called_once_with(f"{base_url}{retl_sync_status_endpoint}", headers={'authorization' : f"Bearer some-password", 'Content-Type': 'application/json'})
        mock_connection.assert_called_with('rudderstack_connection')
    

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.requests.get')
    def test_poll_for_retl_sync_status_failed(self, mock_get: mock.Mock, mock_connection: mock.Mock):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {'status': 'failed'}
        mock_connection.side_effect = [Connection(password='some-password', host='https://some-url.com'), Connection(password='some-password')]
        retl_connection_id = 'some-connection-id'
        sync_id = 'some-sync-id'
        base_url = 'https://some-url.com'
        retl_sync_status_endpoint = f"/v2/retl-connections/{retl_connection_id}/syncs/{sync_id}"
        hook = RudderstackHook('rudderstack_connection')
        self.assertRaises(AirflowException, hook.poll_retl_sync_status, retl_connection_id, sync_id)
        mock_get.assert_called_once_with(f"{base_url}{retl_sync_status_endpoint}", headers={'authorization' : f"Bearer some-password", 'Content-Type': 'application/json'})
        mock_connection.assert_called_with('rudderstack_connection')

    @mock.patch('rudder_airflow_provider.hooks.rudderstack.HttpHook.get_connection')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.requests.get')
    @mock.patch('rudder_airflow_provider.hooks.rudderstack.time.sleep')
    def test_poll_for_retl_sync_status_running(self, mock_sleep: mock.Mock, mock_get: mock.Mock, mock_connection: mock.Mock):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.side_effect = [{'status': 'running'}, {'status': 'succeeded'}]
        mock_connection.side_effect = [Connection(password='some-password', host='https://some-url.com'), Connection(password='some-password')]
        retl_connection_id = 'some-connection-id'
        sync_id = 'some-sync-id'
        base_url = 'https://some-url.com'
        retl_sync_status_endpoint = f"/v2/retl-connections/{retl_connection_id}/syncs/{sync_id}"
        hook = RudderstackHook('rudderstack_connection')
        hook.poll_retl_sync_status(retl_connection_id, sync_id)
        mock_get.assert_called_with(f"{base_url}{retl_sync_status_endpoint}", headers={'authorization' : f"Bearer some-password", 'Content-Type': 'application/json'})
        mock_connection.assert_called_with('rudderstack_connection')
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)
        mock_sleep.assert_called_with(STATUS_POLL_INTERVAL)    


if __name__ == '__main__':
    unittest.main()
