<p align="center">
  <a href="https://rudderstack.com/">
    <img src="https://user-images.githubusercontent.com/59817155/121357083-1c571300-c94f-11eb-8cc7-ce6df13855c9.png">
  </a>
</p>

<p align="center"><b>The Customer Data Platform for Developers</b></p>

<p align="center">
  <b>
    <a href="https://rudderstack.com">Website</a>
    ·
    <a href="https://rudderstack.com/docs/warehouse-actions/airflow-provider/">Documentation</a>
    ·
    <a href="https://rudderstack.com/join-rudderstack-slack-community">Slack Community</a>
  </b>
</p>

---

# RudderStack Airflow Provider

The [RudderStack](https://rudderstack.com) Airflow Provider lets you schedule and trigger your [Warehouse Actions](https://rudderstack.com/docs/warehouse-actions/) syncs from outside RudderStack and integrate them with your existing Airflow workflows.

## Installation

```bash
pip install rudderstack-airflow-provider
```

## Usage

A simple DAG for triggering syncs for a RudderStack source:

```python
with DAG(
    'rudderstack-sample',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['rs']
) as dag:
    rs_operator = RudderstackOperator(
        source_id='<source-id>',
        task_id='<any-task-id>',
        connection_id='rudderstack_conn'
    )
```

For the complete code, refer to this [example](https://github.com/rudderlabs/rudder-airflow-provider/blob/main/examples/sample_dag.py).

### Operator Parameters

| Parameter | Description | Type | Default |
| :--- |:--- | :--- | :--- |
| `source_id` | Valid RudderStack source ID | String | `None` |
| `task_id` | A unique task ID within a DAG | String | `None` |
| `wait_for_completion` | If `True`, the task will wait for sync to complete. | Boolean | `False` |
| `connection_id` | The Airflow connection to use for connecting to the Rudderstack API. | String | `rudderstack_default` |

The RudderStack operator also supports all the parameters supported by the [Airflow base operator](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html).

For details on how to run the DAG in Airflow, refer to the [documentation](https://rudderstack.com/docs/warehouse-actions/airflow-provider/#running-the-dag).

## Contribute

We would love to see you contribute to this project. Get more information on how to contribute [here](CONTRIBUTING.md).

## License

The RudderStack Airflow Provider is released under the [MIT License](LICENSE).

## Contact Us

For more information or queries on this feature, you can [contact us](mailto:%20docs@rudderstack.com) or start a conversation in our [Slack](https://rudderstack.com/join-rudderstack-slack-community) community.
