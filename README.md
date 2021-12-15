<p align="center"><a href="https://rudderstack.com"><img src="https://user-images.githubusercontent.com/59817155/126267034-ae9870b7-9137-4f45-be65-d621b055a972.png" alt="RudderStack - Customer Data Platform for Developers" height="50"/></a></p>
<h1 align="center"></h1>
<p align="center"><b>Customer Data Platform for Developers</b></p>
<br/>


# rudder-airflow-provider
Apache airflow provider for rudderstack. 

> Questions? Start a conversation on our [**Slack channel**][slack].

# Why Use rudder-airflow-provider

Trigger wh-action or cloud extract syncs for rudderstack from apache airflow.


# Installation

```bash
pip install rudderstack-airflow-provider
```

# Usage

A simple DAG for triggering sync for rudderstack source. refer [example](examples/sample_dag.py)

```python
with DAG('rudderstack-sample',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['rs']) as dag:
    rs_operator = RudderstackOperator(source_id='<source-id>', task_id='<any-task-id>')
```
**Operator Params**

| parameter           | description                                                 | type    | default             |
|---------------------|-------------------------------------------------------------|---------|---------------------|
| source_id           | valid rudderstack source id                                 | string  | None                |
| task_id             | a unique task id within a dag                               | string  | None                |
| wait_for_completion | if true task will wait for sync to complete                 | boolean | False               |
| connection_id       | airflow connection to use for connecting to rudderstack api | string  | rudderstack_default |

rudderstack operator supports all the parameters supported by [airflow base operator][https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html]

# License

The rudderstack-airflow-provider is released under the [**MIT License**][mit_license].

# Contribute

We would love to see you contribute to RudderStack. Get more information on how to contribute [**here**](CONTRIBUTING.md).

# Follow Us

- [**RudderStack blog**][rudderstack-blog]
- [**Slack**][slack]
- [**Twitter**][twitter]
- [**LinkedIn**][linkedin]
- [**dev.to**][devto]
- [**Medium**][medium]
- [**YouTube**][youtube]
- [**HackerNews**][hackernews]
- [**Product Hunt**][producthunt]

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
