import logging
from airflow.models import baseoperator
from rudder_airflow_provider.hooks.rudderstack import RudderstackHook


class RudderstackOperator(baseoperator.BaseOperator):
    '''
        Rudderstack operator for airflow DAGs
    '''

    RUDDERTACK_DEFAULT_CONNECTION_ID = 'rudderstack_default'

    def __init__(self, source_id: str, connection_id: str = RUDDERTACK_DEFAULT_CONNECTION_ID,
                 wait_for_completion: bool = False, **kwargs):
        '''
            Initialize rudderstack operator
        '''
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.source_id = source_id
        self.wait_for_completion = wait_for_completion

    def execute(self, context):
        '''
            Executes rudderstack operator
        '''
        rs_hook = RudderstackHook(
            connection_id=self.connection_id, source_id=self.source_id)
        run_id = rs_hook.trigger_sync()
        if self.wait_for_completion and run_id is not None:
            logging.info('waiting for sync to complete for sourceId: %s, runId: %s', self.source_id, run_id)
            rs_hook.poll_for_status(run_id)
