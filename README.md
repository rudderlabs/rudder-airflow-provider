<p align="center"><a href="https://rudderstack.com"><img src="https://user-images.githubusercontent.com/59817155/126267034-ae9870b7-9137-4f45-be65-d621b055a972.png" alt="RudderStack - Customer Data Platform for Developers" height="50"/></a></p>
<h1 align="center"></h1>
<p align="center"><b>The Customer Data Platform for Developers</b></p>
<br/>

<p align="center">
  <b>
    <a href="https://rudderstack.com">Website</a>
    ·
    <a href="https://rudderstack.com/docs/warehouse-actions/airflow-provider/">Documentation</a>
    ·
    <a href="https://rudderstack.com/join-rudderstack-slack-community">Community Slack</a>
  </b>
</p>


# RudderStack Airflow Provider

RudderStack's Airflow Provider lets you schedule and trigger your [**Warehouse Actions**](https://rudderstack.com/docs/warehouse-actions/) syncs from outside RudderStack and integrate them with your existing Airflow workflows.


| Refer to our detailed technical [**documentation**](https://rudderstack.com/docs/warehouse-actions/airflow-provider/) for more information on this feature. If you have any questions, you can start a conversation on our [**Slack channel**][slack]. |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |

## Installation

```bash
pip install rudderstack-airflow-provider
```

## Usage

A simple DAG for triggering sync for the RudderStack source. For the complete code, refer to this [**example**](examples/sample_dag.py).

```python
with DAG('rudderstack-sample',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['rs']) as dag:
    rs_operator = RudderstackOperator(source_id='<source-id>', task_id='<any-task-id>', connection_id='rudderstack_conn')
```
### Operator parameters

| Parameter             | Description                                                          | Type    | Default               |
|:----------------------|:-------------------------------------------------------------------- |:--------|:----------------------|
| `source_id`           | Valid RudderStack source ID                                          | String  | None                  |
| `task_id`             | A unique task ID within a DAG                                        | String  | None                  |
| `wait_for_completion` | If `True`, the task will wait for sync to complete.                  | Boolean | False                 |
| `connection_id`       | The Airflow connection to use for connecting to the Rudderstack API. | String  | `rudderstack_default` |

The RudderStack operator supports all the parameters supported by the [**Airflow base operator**](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html).

For details on how to run the DAG in Airflow, refer to the  [**documentation**](https://rudderstack.com/docs/warehouse-actions/airflow-provider).

## License

The RudderStack Airflow Provider is released under the [**MIT License**][mit_license].

## Contribute

We would love to see you contribute to this project. Get more information on how to contribute [**here**](CONTRIBUTING.md).

## Contact us

For more information or queries on this feature, you can [**contact us**](mailto:%20docs@rudderstack.com) or start a conversation on our [**Slack**](https://rudderstack.com/join-rudderstack-slack-community) channel.

<!----variables---->

[slack]: https://rudderstack.com/join-rudderstack-slack-community
[twitter]: https://twitter.com/rudderstack
[linkedin]: https://www.linkedin.com/company/rudderlabs/
[devto]: https://dev.to/rudderstack
[medium]: https://rudderstack.medium.com/
[youtube]: https://www.youtube.com/channel/UCgV-B77bV_-LOmKYHw8jvBw
[rudderstack-blog]: https://rudderstack.com/blog/
[hackernews]: https://news.ycombinator.com/item?id=21081756
[producthunt]: https://www.producthunt.com/posts/rudderstack
[mit_license]: https://opensource.org/licenses/MIT
[agplv3_license]: https://www.gnu.org/licenses/agpl-3.0-standalone.html
[sspl_license]: https://www.mongodb.com/licensing/server-side-public-license
[config-generator]: https://github.com/rudderlabs/config-generator
[config-generator-section]: https://github.com/rudderlabs/rudder-server/blob/master/README.md#rudderstack-config-generator
[rudder-logo]: https://repository-images.githubusercontent.com/197743848/b352c900-dbc8-11e9-9d45-4deb9274101f
