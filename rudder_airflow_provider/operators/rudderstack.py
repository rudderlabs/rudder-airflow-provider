import logging
from airflow.models import baseoperator
from rudder_airflow_provider.hooks.rudderstack import RudderstackHook

RUDDERTACK_DEFAULT_CONNECTION_ID = 'rudderstack_default'
class RudderstackOperator(baseoperator.BaseOperator):
    '''
        Rudderstack operator for airflow DAGs
    '''
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
        rs_hook = RudderstackHook(connection_id=self.connection_id)
        run_id = rs_hook.trigger_sync(self.source_id)
        if self.wait_for_completion and run_id is not None:
            logging.info('waiting for sync to complete for sourceId: %s, runId: %s', self.source_id, run_id)
            rs_hook.poll_for_status(self.source_id, run_id)


class RudderstackRETLOperator(baseoperator.BaseOperator):
    template_fields = ('retl_connection_id', 'sync_type')
    
    '''
        Rudderstack operator for RETL connnections
        :param retl_connection_id: unique id of the retl connection
        :param sync_type: type of sync to trigger. Possible values are 'full' or 'incremental'
        :param connection_id: airflow connection id for rudderstack API
        :param wait_for_completion: wait for sync to complete. Default is False
    '''
    def __init__(self, 
                 retl_connection_id: str, 
                 sync_type: str, 
                 connection_id: str = RUDDERTACK_DEFAULT_CONNECTION_ID,
                 wait_for_completion: bool = False,
                 **kwargs):
        '''
            Initialize rudderstack operator
        '''
        super().__init__(**kwargs)
        self.wait_for_completion = wait_for_completion
        self.connection_id = connection_id
        self.retl_connection_id = retl_connection_id
        self.sync_type = sync_type

    def execute(self, context):
        rs_hook = RudderstackHook(connection_id=self.connection_id)
        sync_id = rs_hook.trigger_retl_sync(self.retl_connection_id, self.sync_type)
        if self.wait_for_completion:
            logging.info('waiting for sync to complete for retl-connecion: %s, syncId: %s', self.retl_connection_id, sync_id)
            rs_hook.poll_retl_sync_status(self.retl_connection_id, sync_id)