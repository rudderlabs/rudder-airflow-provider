import logging
import time
import datetime
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urljoin
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
import requests

DEFAULT_POLL_INTERVAL_SECONDS = 10
DEFAULT_REQUEST_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1
DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_RUDDERSTACK_API_ENDPOINT = "https://api.rudderstack.com"


class RETLSyncStatus:
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RETLSyncType:
    INCREMENTAL = "incremental"
    FULL = "full"


class ProfilesRunStatus:
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


class BaseRudderStackHook(HttpHook):
    """
    BaseRudderStackHook to interact with RudderStack API.
    :params connection_id: `Conn ID` of the Connection to be used to configure this hook.
    :params request_retry_delay: Time (in seconds) to wait between each request retry..
    :params request_timeout: Time (in seconds) after which the requests to RudderStack are declared timed out.
    :params request_max_retries: The maximum number of times requests to the RudderStack API should be retried before failng.
    """

    def __init__(
        self,
        connection_id: str,
        request_retry_delay: int = 1,
        request_timeout: int = 30,
        request_max_retries: int = 3,
    ) -> None:
        self.connection_id = connection_id
        self.request_retry_delay = request_retry_delay
        self.request_timeout = request_timeout
        self.request_max_retries = request_max_retries
        super().__init__(http_conn_id=self.connection_id)

    def _get_access_token(self) -> str:
        """
        returns rudderstack access token
        """
        conn = self.get_connection(self.connection_id)
        return conn.password

    def _get_api_base_url(self):
        """
        returns base api url
        """
        conn = self.get_connection(self.connection_id)
        return conn.host

    def _get_request_headers(self) -> dict:
        """
        Returns the request headers to be used by the hook.
        """
        access_token = self._get_access_token()
        return {
            "authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    def make_request(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[Mapping[str, object]] = None,
    ):
        """Prepares and makes request to RudderStack API endpoint.

        Args:
            method (str): The http method to be used for this request (e.g. "GET", "POST").
            endpoint (str): The RudderStack API endpoint to send request to.
            data (Optional(Mapping)): Data to pass in request to the API endpoint.

        Returns:
            Dict[str, Any]: Parsed json data from the response for this request.
        """
        url = urljoin(self._get_api_base_url(), endpoint)
        headers = self._get_request_headers()
        num_retries = 0
        while True:
            try:
                request_args: Dict[str, Any] = dict(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=self.request_timeout,
                )
                if data is not None:
                    request_args["json"] = data
                response = requests.request(**request_args)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                logging.error(f"Request to url: {url} failed: {e}")
                if num_retries == self.request_max_retries:
                    break
                num_retries += 1
                time.sleep(self.request_retry_delay)

        raise AirflowException(
            f"Exceeded max number of retries for connectionId: {self.connection_id}"
        )


class RudderStackRETLHook(BaseRudderStackHook):
    """
    RudderStackRETLHook to interact with RudderStack RETL API.
    :params connection_id: `Conn ID` of the Connection to be used to configure this hook.
    :params request_retry_delay: Time (in seconds) to wait between each request retry..
    :params request_timeout: Time (in seconds) after which the requests to RudderStack are declared timed out.
    :params request_max_retries: The maximum number of times requests to the RudderStack API should be retried before failng.
    """

    def __init__(
        self,
        connection_id: str,
        request_retry_delay: int = DEFAULT_RETRY_DELAY,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        request_max_retries: int = DEFAULT_REQUEST_MAX_RETRIES,
        poll_timeout: float = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> None:
        super().__init__(
            connection_id, request_retry_delay, request_timeout, request_max_retries
        )
        self.poll_timeout = poll_timeout
        self.poll_interval = poll_interval

    def start_sync(self, retl_connection_id, sync_type: Optional[str] = None) -> str:
        """Triggers a sync and returns runId if successful, else raises Failure.

        Args:
            retl_connection_id (str): connetionId for an RETL sync.
            sync_type (str): (optional) full or incremental. Default is None.

        Returns:
            sync_id of the sync started.
        """
        if sync_type is not None and sync_type not in [
            RETLSyncType.INCREMENTAL,
            RETLSyncType.FULL,
        ]:
            raise AirflowException(f"Invalid sync type: {sync_type}")
        if not retl_connection_id:
            raise AirflowException("retl_connection_id is required")
        self.log.info("Triggering sync for retl connection id: %s", retl_connection_id)

        data = {}
        if sync_type is not None:
            data = {"syncType": sync_type}
        return self.make_request(
            endpoint=f"/v2/retl-connections/{retl_connection_id}/start",
            data=data,
        )["syncId"]

    def poll_sync(self, retl_connection_id, sync_id: str) -> Dict[str, Any]:
        """Polls for completion of a sync. If poll_timeout is set, raises Failure after timeout.

        Args:
            retl_connection_id (str): connetionId for an RETL sync.
            sync_type (str): (optional) full or incremental. Default is None.
        Returns:
            Dict[str, Any]: Parsed json output from syncs endpoint.
        """
        if not retl_connection_id:
            raise AirflowException(
                "retl_connection_id is required to poll status of sync run"
            )
        if not sync_id:
            raise AirflowException("sync_id is required to poll status of sync run")

        status_endpoint = f"/v2/retl-connections/{retl_connection_id}/syncs/{sync_id}"
        poll_start = datetime.datetime.now()
        while True:
            resp = self.make_request(endpoint=status_endpoint, method="GET")
            sync_status = resp["status"]
            self.log.info(
                f"Polled status for syncId: {sync_id} for retl connection: {retl_connection_id}, status: {sync_status}"
            )
            if sync_status == RETLSyncStatus.SUCCEEDED:
                self._log.info(
                    f"Sync finished for retl connection: {retl_connection_id}, syncId: {sync_id}"
                )
                return resp
            elif sync_status == RETLSyncStatus.FAILED:
                error_msg = resp.get("error", None)
                raise AirflowException(
                    f"Sync for retl connection: {retl_connection_id}, syncId: {sync_id} failed with error: {error_msg}"
                )
            if (
                self.poll_timeout
                and datetime.datetime.now()
                > poll_start + datetime.timedelta(seconds=self.poll_timeout)
            ):
                raise AirflowException(
                    f"Polling for syncId: {sync_id} for retl connection: {retl_connection_id} timed out"
                )
            time.sleep(self.poll_interval)


class RudderStackProfilesHook(BaseRudderStackHook):
    """
    RudderStackRETLHook to interact with RudderStack RETL API.
    :params connection_id: `Conn ID` of the Connection to be used to configure this hook.
    :params request_retry_delay: Time (in seconds) to wait between each request retry..
    :params request_timeout: Time (in seconds) after which the requests to RudderStack are declared timed out.
    :params request_max_retries: The maximum number of times requests to the RudderStack API should be retried before failng.
    """

    def __init__(
        self,
        connection_id: str,
        request_retry_delay: int = DEFAULT_RETRY_DELAY,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        request_max_retries: int = DEFAULT_REQUEST_MAX_RETRIES,
        poll_timeout: float = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> None:
        super().__init__(
            connection_id, request_retry_delay, request_timeout, request_max_retries
        )
        self.poll_timeout = poll_timeout
        self.poll_interval = poll_interval

    def start_profile_run(self, profile_id: str):
        """Triggers a profile run and returns runId if successful, else raises Failure.

        Args:
            profile_id (str): Profile ID
        """
        if not profile_id:
            raise AirflowException("profile_id is required to start a profile run")
        self.log.info(f"Triggering profile run for profile id: {profile_id}")
        return self.make_request(
            endpoint=f"/v2/sources/{profile_id}/start",
        )["runId"]

    def poll_profile_run(self, profile_id: str, run_id: str) -> Dict[str, Any]:
        """Polls for completion of a profile run. If poll_timeout is set, raises Failure after timeout.

        Args:
            profile_id (str): Profile ID
            run_id (str): Run ID
        Returns:
            Dict[str, Any]: Parsed json output from profile run endpoint.
        """
        if not profile_id:
            raise AirflowException("profile_id is required to start a profile run")
        if not run_id:
            raise AirflowException("run_id is required to poll status of profile run")

        status_endpoint = f"/v2/sources/{profile_id}/runs/{run_id}/status"
        poll_start = datetime.datetime.now()
        while True:
            resp = self.make_request(endpoint=status_endpoint, method="GET")
            run_status = resp["status"]
            self.log.info(
                f"Polled status for runId: {run_id} for profile: {profile_id}, status: {run_status}"
            )
            if run_status == ProfilesRunStatus.FINISHED:
                self.log.info(
                    f"Profile run finished for profile: {profile_id}, runId: {run_id}"
                )
                return resp
            elif run_status == ProfilesRunStatus.FAILED:
                error_msg = resp.get("error", None)
                raise AirflowException(
                    f"Profile run for profile: {profile_id}, runId: {run_id} failed with error: {error_msg}"
                )
            if (
                self.poll_timeout
                and datetime.datetime.now()
                > poll_start + datetime.timedelta(seconds=self.poll_timeout)
            ):
                raise AirflowException(
                    f"Polling for runId: {run_id} for profile: {profile_id} timed out"
                )
            time.sleep(self.poll_interval)
