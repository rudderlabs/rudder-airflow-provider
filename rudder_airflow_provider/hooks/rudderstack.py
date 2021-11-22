import logging
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook

class RudderstackHook(HttpHook):
    def __init__(self,  connection_id: str, source_id: str) -> None:
        self.connection_id = connection_id
        if source_id is None:
            raise AirflowException('source_id is mandatory')
        self.source_id = source_id
        super().__init__(http_conn_id=self.connection_id)
    
    def trigger_sync(self):
        self.method = 'POST'
        sync_endpoint = f"/v2/sources/{self.source_id}/start"
        access_token = self.get_access_token()
        logging.info(access_token)
        logging.info('triggering sync for sourceId: %s,  endpoint: %s', self.source_id, sync_endpoint)
        return self.run(endpoint=sync_endpoint, headers={'authorization': f"Bearer {access_token}"})
    
    def get_access_token(self) -> str:
        conn = self.get_connection(self.connection_id)
        return conn.password