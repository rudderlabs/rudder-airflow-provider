from airflow.models import baseoperator
from sqlalchemy.orm import selectin_polymorphic
from rudder_airflow_provider.hooks.rudderstack import RudderstackHook


class RudderstackOperator(baseoperator.BaseOperator): 

    RUDDERTACK_DEFAULT_CONNECTION_ID = 'rudderstack_default'

    def __init__(self, source_id: str, connection_id: str = RUDDERTACK_DEFAULT_CONNECTION_ID, **kwargs):
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.source_id = source_id
    
    def execute(self, context):
        rs_hook = RudderstackHook(self.connection_id, self.source_id)
        rs_hook.trigger_sync()
        return