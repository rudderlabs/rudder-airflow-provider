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
    <a href="https://rudderstack.com/join-rudderstack-slack-community">Slack Community</a>
  </b>
</p>

---

# RudderStack Airflow Provider

The [RudderStack](https://rudderstack.com) Airflow Provider lets you programmatically schedule and trigger your [Reverse ETL](https://www.rudderstack.com/docs/reverse-etl) syncs and Profiles runs outside RudderStack and integrate them with your existing Airflow workflows.


## Installation

```bash
pip install rudderstack-airflow-provider
```

## Usage

### RudderstackRETLOperator

> [!NOTE]  
> Use [RudderstackRETLOperator](#rudderstackretloperator) for reverse ETL connections

A simple DAG for triggering syncs for a RudderStack Reverse ETL source:

```python
with DAG(
    "rudderstack-retl-sample",
    default_args=default_args,
    description="A simple tutorial DAG for reverse etl",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["rs-retl"],
) as dag:
    # retl_connection_id, sync_type are template fields
    rs_operator = RudderstackRETLOperator(
        retl_connection_id="connection_id",
        task_id="<replace task id>",
        connection_id="<rudderstack api airflow connection id>"
    )
```

For the complete code, refer to this [example](https://github.com/rudderlabs/rudder-airflow-provider/tree/main/examples).

Mandatatory parameters for RudderstackRETLOperator:
* retl_connection_id: This is the [connection id](https://www.rudderstack.com/docs/sources/reverse-etl/airflow-provider/#where-can-i-find-the-connection-id-for-my-reverse-etl-connection) for the sync job.
* connection_id: The Airflow connection to use for connecting to the Rudderstack API.	Default value is `rudderstack_default`.


RudderstackRETLOperator exposes other configurable parameters as well. Mostly default values for them would be recommended.

* request_max_retries: The maximum number of times requests to the RudderStack API should be retried before failng.
* request_retry_delay: Time (in seconds) to wait between each request retry.
* request_timeout: Time (in seconds) after which the requests to RudderStack are declared timed out.
* poll_interval: Time (in seconds) for polling status of triggered job.
* poll_timeout: Time (in seconds) after which the polling for a triggered job is declared timed out.
* wait_for_completion: Boolean if execution run should poll and wait till completion of sync. Default value is True.
* sync_type: Type of sync to trigger `incremental` or `full`. Default is None as RudderStack will be deteriming sync type.


### RudderstackProfilesOperator

RudderstackProfilesOperator can be used to trigger profiles run. A simple DAG for triggering profile runs for a profiles project.

```python
with DAG(
    "rudderstack-profiles-sample",
    default_args=default_args,
    description="A simple tutorial DAG for profiles run.",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["rs-profiles"],
) as dag:
    # profile_id is template field
    rs_operator = RudderstackProfilesOperator(
        profile_id="<profile_id>",
        task_id="<replace task id>",
        connection_id="<rudderstack api connection id>",
    )
```

Mandatatory parameters for RudderstackProfilesOperator:
* profile_id: This is the [profiles id](https://www.rudderstack.com/docs/api/profiles-api/#run-project) for the profiles project to run.
* connection_id: The Airflow connection to use for connecting to the Rudderstack API.	Default value is `rudderstack_default`.

RudderstackProfilesOperator exposes other configurable parameters as well. Mostly default values for them would be recommended.

* request_max_retries: The maximum number of times requests to the RudderStack API should be retried before failng.
* request_retry_delay: Time (in seconds) to wait between each request retry.
* request_timeout: Time (in seconds) after which the requests to RudderStack are declared timed out.
* poll_interval: Time (in seconds) for polling status of triggered job.
* poll_timeout: Time (in seconds) after which the polling for a triggered job is declared timed out.
* wait_for_completion: Boolean if execution run should poll and wait till completion of sync. Default value is True.
* parameters: Additional parameters to pass to the profiles run command, as supported by the API endpoint. Default value is `None`.


## Contribute

We would love to see you contribute to this project. Get more information on how to contribute [here](CONTRIBUTING.md).

## License

The RudderStack Airflow Provider is released under the [MIT License](LICENSE).

## Contact Us

For more information or queries on this feature, you can [contact us](mailto:%20docs@rudderstack.com) or start a conversation in our [Slack](https://rudderstack.com/join-rudderstack-slack-community) community.
