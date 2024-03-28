from enum import Enum
import logging
import time

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
import requests

STATUS_FINISHED = 'finished'
STATUS_POLL_INTERVAL = 10

class RETLSyncStatus(Enum):
    '''
        Enum for retl sync status
    '''
    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'

class RudderstackHook(HttpHook):
    '''
        Hook for rudderstack public API
    '''

    def __init__(self,  connection_id: str) -> None:
        self.connection_id = connection_id
        super().__init__(http_conn_id=self.connection_id)

    def trigger_sync(self, source_id:str) -> str | None:
        '''
            trigger sync for a source
        '''
        self.method = 'POST'
        sync_endpoint = f"/v2/sources/{source_id}/start"
        headers = self.get_request_headers()
        logging.info('triggering sync for sourceId: %s, endpoint: %s',
                     source_id, sync_endpoint)
        resp = self.run(endpoint=sync_endpoint, headers=headers,
            extra_options={"check_response": False})
        if resp.status_code in (200, 204, 201):
            logging.info('Job triggered for sourceId: %s', source_id)
            return resp.json().get('runId')
        elif resp.status_code == 409:
            logging.info('Job is already running for sourceId: %s', source_id)
        else:
            raise AirflowException(f"Error while starting sync for sourceId: {source_id}, response: {resp.status_code}")

    def poll_for_status(self, source_id, run_id: str):
        '''
            polls for sync status
        '''
        status_endpoint = f"/v2/sources/{source_id}/runs/{run_id}/status"
        headers = self.get_request_headers()
        while True:
            self.method = 'GET'
            resp = self.run(endpoint=status_endpoint, headers=headers).json()
            job_status = resp['status']
            logging.info('sync status for sourceId: %s, runId: %s, status: %s',
                         source_id, run_id, job_status)

            if job_status == STATUS_FINISHED:
                if resp.get('error'):
                    raise AirflowException(
                        f"sync for sourceId: {source_id} failed with error: {resp['error']}")

                logging.info('sync finished for sourceId: %s, runId: %s', source_id, run_id)
                break
            time.sleep(STATUS_POLL_INTERVAL)

    def get_request_headers(self) -> dict:
        access_token = self.get_access_token()
        return {
            'authorization': f"Bearer {access_token}",
            'Content-Type': 'application/json'
        }

    def get_access_token(self) -> str:
        '''
            returns rudderstack access token
        '''
        conn = self.get_connection(self.connection_id)
        return conn.password

    def get_api_base_url(self):
        '''
            returns base api url
        '''
        conn = self.get_connection(self.connection_id)
        return conn.host

    def trigger_retl_sync(self, retl_connection_id, sync_type: str):
        '''
            trigger sync for a retl source
        '''
        base_url = self.get_api_base_url().rstrip('/')
        sync_endpoint = f"/v2/retl-connections/{retl_connection_id}/start"
        headers = self.get_request_headers()
        logging.info('triggering sync for retl connection, endpoint: %s', sync_endpoint)
        sync_req_data = { "syncType": sync_type }
        resp = requests.post(f"{base_url}{sync_endpoint}", json=sync_req_data, headers=headers)
        if resp.status_code == 200:
            logging.info('Job triggered for retl connection, syncId: %s', resp.json().get("syncId"))
            return resp.json().get('syncId')
        else:
            error = resp.json().get('error', None)
            raise AirflowException(f"Error while starting sync for retl, response: {resp.status_code}, error: {error}")
    

    def poll_retl_sync_status(self, retl_connection_id, sync_id: str):
        '''
            polls for retl sync status
        '''
        base_url = self.get_api_base_url().rstrip('/')
        status_endpoint = f"/v2/retl-connections/{retl_connection_id}/syncs/{sync_id}"
        headers = self.get_request_headers()
        while True:
            resp = requests.get(f"{base_url}{status_endpoint}", headers=headers)
            if resp.status_code != 200:
                error_msg = resp.json().get('error', None)
                raise AirflowException(f"Error while fetching sync status for retl, status: {resp.status_code}, error: {error_msg}")
            job_status = resp.json()['status']
            logging.info('sync status for retl connection, sycId: %s, status: %s', sync_id, job_status)
            if job_status == RETLSyncStatus.SUCCEEDED.value:
                logging.info('sync finished for retl connection, syncId: %s', sync_id)
                break
            elif job_status == RETLSyncStatus.FAILED.value:
                error_msg = resp.json().get('error', None)
                raise AirflowException(f"sync for retl connection failed with error: {error_msg}")
            time.sleep(STATUS_POLL_INTERVAL)