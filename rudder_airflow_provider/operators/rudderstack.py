import logging
from airflow.models import baseoperator
from typing import Optional, List
from rudder_airflow_provider.hooks.rudderstack import (
    RudderStackRETLHook,
    RudderStackProfilesHook,
    DEFAULT_REQUEST_MAX_RETRIES,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_RETRY_DELAY,
    DEFAULT_REQUEST_TIMEOUT,
)

RUDDERTACK_DEFAULT_CONNECTION_ID = "rudderstack_default"


class RudderstackRETLOperator(baseoperator.BaseOperator):
    template_fields = "retl_connection_id"

    """
        Rudderstack operator for RETL connnections
        :param retl_connection_id: unique id of the retl connection
        :param sync_type: type of sync to trigger. Default is None and is recommended. Possible values are incremental or full.
        :param connection_id: airflow connection id for rudderstack API
        :param wait_for_completion: wait for sync to complete. Default is True.
    """

    def __init__(
        self,
        retl_connection_id: str,
        sync_type: Optional[str] = None,
        connection_id: str = RUDDERTACK_DEFAULT_CONNECTION_ID,
        wait_for_completion: bool = True,
        request_retry_delay: int = DEFAULT_RETRY_DELAY,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        request_max_retries: int = DEFAULT_REQUEST_MAX_RETRIES,
        poll_timeout: float = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
        **kwargs,
    ):
        """
        Initialize rudderstack operator
        """
        super().__init__(**kwargs)
        self.wait_for_completion = wait_for_completion
        self.connection_id = connection_id
        self.retl_connection_id = retl_connection_id
        self.sync_type = sync_type
        self.request_retry_delay = request_retry_delay
        self.request_timeout = request_timeout
        self.request_max_retries = request_max_retries
        self.poll_timeout = poll_timeout
        self.poll_interval = poll_interval

    def execute(self, context):
        rs_hook = RudderStackRETLHook(
            connection_id=self.connection_id,
            request_retry_delay=self.request_retry_delay,
            request_timeout=self.request_timeout,
            request_max_retries=self.request_max_retries,
            poll_timeout=self.poll_timeout,
            poll_interval=self.poll_interval,
        )
        sync_id = rs_hook.start_sync(self.retl_connection_id, self.sync_type)
        if self.wait_for_completion:
            self.log.info(
                f"poll and wait for sync to finish for retl-connectionId: {self.retl_connection_id}, syncId: {sync_id}"
            )
            rs_hook.poll_sync(self.retl_connection_id, sync_id)


class RudderstackProfilesOperator(baseoperator.BaseOperator):
    template_fields = ("profile_id", "parameters")

    """
        Rudderstack operator for Profiles
        :param profile_id: profile id to trigger
        :param wait_for_completion: wait for sync to complete. Default is True
    """

    def __init__(
        self,
        profile_id: str,
        connection_id: str = RUDDERTACK_DEFAULT_CONNECTION_ID,
        wait_for_completion: bool = True,
        request_retry_delay: int = DEFAULT_RETRY_DELAY,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        request_max_retries: int = DEFAULT_REQUEST_MAX_RETRIES,
        poll_timeout: float = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
        parameters: Optional[List[str]] = None,
        **kwargs,
    ):
        """
        Initialize rudderstack operator
        """
        super().__init__(**kwargs)
        self.wait_for_completion = wait_for_completion
        self.connection_id = connection_id
        self.profile_id = profile_id
        self.request_retry_delay = request_retry_delay
        self.request_timeout = request_timeout
        self.request_max_retries = request_max_retries
        self.poll_timeout = poll_timeout
        self.poll_interval = poll_interval
        self.parameters = parameters

    def execute(self, context):
        rs_profiles_hook = RudderStackProfilesHook(
            connection_id=self.connection_id,
            request_retry_delay=self.request_retry_delay,
            request_timeout=self.request_timeout,
            request_max_retries=self.request_max_retries,
            poll_timeout=self.poll_timeout,
            poll_interval=self.poll_interval,
        )
        profile_run_id = rs_profiles_hook.start_profile_run(self.profile_id, self.parameters)
        if self.wait_for_completion:
            self.log.info(
                f"Poll and wait for profiles run to finish for profilesId: {self.profile_id}, runId: {profile_run_id}"
            )
            rs_profiles_hook.poll_profile_run(self.profile_id, profile_run_id)
