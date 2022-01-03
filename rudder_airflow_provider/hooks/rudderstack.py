import logging
import time

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from requests.models import Response

STATUS_FINISHED = 'finished'
STATUS_POLL_INTERVAL = 10


class RudderstackHook(HttpHook):
    '''
        Hook for rudderstack public API
    '''

    def __init__(self,  connection_id: str, source_id: str) -> None:
        self.connection_id = connection_id
        if source_id is None:
            raise AirflowException('source_id is mandatory')
        self.source_id = source_id
        super().__init__(http_conn_id=self.connection_id)

    def trigger_sync(self):
        '''
            trigger sync for a source
        '''
        self.method = 'POST'
        sync_endpoint = f"/v2/sources/{self.source_id}/start"
        access_token = self.get_access_token()
        logging.info('triggering sync for sourceId: %s, endpoint: %s',
                     self.source_id, sync_endpoint)
        resp = self.run(endpoint=sync_endpoint, headers={
                'authorization': f"Bearer {access_token}"}, extra_options={"check_response": False})
        if resp.status_code in (200, 204, 201):
            logging.info('Job triggered for sourceId: %s', self.source_id)
        elif resp.status_code == 409:
            logging.info('Job is already running for sourceId: %s', self.source_id)
        else:
            raise AirflowException(f"Error while starting sync for sourceId: {self.source_id}, response: {resp.status_code}")

    def poll_for_status(self):
        '''
            polls for sync status
        '''
        status_endpoint = f"/v2/sources/{self.source_id}/status"
        access_token = self.get_access_token()
        while True:
            self.method = 'GET'
            resp = self.run(endpoint=status_endpoint, headers={
                            'authorization': f"Bearer {access_token}"}).json()
            job_status = resp['status']
            logging.info('sync status for sourceId: %s, status: %s',
                         self.source_id, job_status)

            if job_status == STATUS_FINISHED:
                if resp.get('error'):
                    raise AirflowException(
                        f"sync for sourceId: {self.source_id} failed with error: {resp['error']}")

                logging.info('sync finished for sourceId: %s', self.source_id)
                break
            time.sleep(STATUS_POLL_INTERVAL)

    def get_access_token(self) -> str:
        '''
            returns rudderstack access token
        '''
        conn = self.get_connection(self.connection_id)
        return conn.password
