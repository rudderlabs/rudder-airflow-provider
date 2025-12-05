# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **RudderStack Airflow Provider** - an Apache Airflow provider package that enables orchestration of RudderStack data pipelines. It allows users to trigger and monitor:
- **Reverse ETL (RETL)** syncs
- **Profiles** runs (customer data platform processing)
- **ETL** source syncs

The provider is published to PyPI as `rudderstack-airflow-provider` and integrates with Airflow's provider system.

## Development Commands

### Install for Development
```bash
# Install in editable mode
pip install -e .

# Or install from scratch
pip install .
```

### Testing
```bash
# Run all tests with coverage
make test

# Run tests directly with pytest
pytest rudder_airflow_provider/test/

# Run specific test file
pytest rudder_airflow_provider/test/hooks/test_rudderstack_hook.py

# Run specific test function
pytest rudder_airflow_provider/test/hooks/test_rudderstack_hook.py::test_get_access_token
```

### Linting
```bash
# Run ruff linter
make lint

# Or directly
ruff check rudder_airflow_provider/*.py
```

### Building
```bash
# Build distribution packages (wheel and source tarball)
python3 -m build

# Output will be in dist/ directory
```

## Code Architecture

### Hook-Operator Pattern

The provider follows Airflow's standard **hook-operator pattern**:

1. **Hooks** (`rudder_airflow_provider/hooks/rudderstack.py`) - Handle API communication and business logic
2. **Operators** (`rudder_airflow_provider/operators/rudderstack.py`) - Provide DAG-level interface for users

### Hook Hierarchy

```
BaseRudderStackHook (abstract)
├── Common functionality: authentication, request handling, retries
├── RudderStackRETLHook - Reverse ETL operations
├── RudderStackProfilesHook - Profiles operations
└── RudderStackETLHook - ETL operations
```

**BaseRudderStackHook** provides:
- `_get_access_token()` - Retrieves bearer token from Airflow connection
- `_get_api_base_url()` - Gets API endpoint (default: https://api.rudderstack.com)
- `_get_request_headers()` - Builds headers with auth and User-Agent
- `make_request()` - HTTP request wrapper with retry logic

**Child hooks** implement:
- `start_sync()` / `start_profile_run()` - Initiates async operations
- `poll_sync()` / `poll_profile_run()` - Polls until completion or timeout

### Operators

Three operator classes corresponding to each hook:
- `RudderstackRETLOperator` - Triggers RETL syncs (incremental or full)
- `RudderstackProfilesOperator` - Triggers Profiles runs (with optional parameters)
- `RudderstackETLOperator` - Triggers ETL source syncs

All operators:
- Use `connection_id` (default: `"rudderstack_default"`) to reference Airflow HTTP connection
- Support `wait_for_completion` (default: True) to poll until done
- Expose configurable retry/timeout parameters

### Template Fields

Operators use Airflow's Jinja templating:
- `RudderstackRETLOperator`: `retl_connection_id` (templatable)
- `RudderstackProfilesOperator`: `profile_id`, `parameters` (templatable)
- `RudderstackETLOperator`: `etl_source_id` (templatable)

This allows users to use `{{ var.value.retl_connection_id }}` or other Jinja expressions in DAGs.

### Connection Configuration

Users configure RudderStack API access via Airflow Connections:
- **Connection Type**: HTTP
- **Host**: RudderStack API endpoint (e.g., `https://api.rudderstack.com`)
- **Password**: RudderStack Access Token (bearer token)

The provider uses `HttpHook` as its base, leveraging Airflow's connection management.

### Request Flow

1. Operator `execute()` creates appropriate Hook instance
2. Hook calls `start_sync()` → returns run/sync ID
3. If `wait_for_completion=True`, Hook calls `poll_sync()` in loop:
   - Checks status via GET request
   - Sleeps for `poll_interval` seconds (default: 10s)
   - Raises `AirflowException` on failure or timeout
   - Returns response dict on success

### Status Enums

The provider defines status constants as class attributes (not true enums):
- `RETLSyncStatus`: `RUNNING`, `SUCCEEDED`, `FAILED`
- `ProfilesRunStatus`: `RUNNING`, `FINISHED`, `FAILED`
- `ETLRunStatus`: `RUNNING`, `FINISHED`, `FAILED`

### API Endpoints

RETL:
- Start: `POST /v2/retl-connections/{id}/start`
- Status: `GET /v2/retl-connections/{id}/syncs/{syncId}`

Profiles & ETL:
- Start: `POST /v2/sources/{id}/start`
- Status: `GET /v2/sources/{id}/runs/{runId}/status`

### Error Handling

- Network errors trigger exponential retry (up to `request_max_retries`, default: 3)
- API failures (4xx/5xx) raise `AirflowException` after retries exhausted
- Poll timeout raises `AirflowException` if `poll_timeout` exceeded
- Failed syncs (status=failed) raise `AirflowException` with error message from API

#### Failure Detection (PRO-4785 Fix)

The hooks implement comprehensive failure detection to ensure Airflow tasks properly fail when RudderStack operations fail:

**Profiles & ETL Hooks:**
- When polling returns `status="finished"`, the hooks check for an `error` field in the response
- If `error` field exists and is non-null/non-empty, raises `AirflowException` with the error message
- This catches cases where the API returns "finished" status but the operation failed due to configuration issues, authentication errors, etc.
- Implementation:
  ```python
  if run_status == ProfilesRunStatus.FINISHED:
      error_msg = resp.get("error", None)
      if error_msg:
          raise AirflowException(f"Profile run for profile: {profile_id}, runId: {run_id} failed with error: {error_msg}")
      return resp
  ```

**RETL Hook:**
- Uses explicit `SUCCEEDED` and `FAILED` status values
- When `status="failed"`, raises `AirflowException` with error message from the response
- No additional error field checking needed since status is explicit

**Test Coverage:**
- `test_poll_profile_run_finished_with_error` - Validates Profiles error detection
- `test_poll_profile_run_finished_no_error` - Validates Profiles success case
- `test_poll_retl_sync_failed` - Validates RETL failure detection
- `test_poll_retl_sync_failed_no_error_message` - Validates RETL failure without error message
- `test_poll_etl_sync_finished_with_error` - Validates ETL error detection
- `test_poll_etl_sync_finished_no_error` - Validates ETL success case

## Testing Strategy

Tests use:
- **pytest** as test runner
- **unittest.mock** for mocking Airflow connections and API responses
- **responses** library for HTTP mocking (installed as dependency)
- **pytest-cov** for coverage reporting

Test structure mirrors source:
- `rudder_airflow_provider/test/hooks/test_rudderstack_hook.py`
- `rudder_airflow_provider/test/operators/test_rudderstack_operator.py`

Key patterns:
- Mock `HttpHook.get_connection()` to return test Connection objects
- Mock hook methods when testing operators
- Use `@patch` decorators for dependency injection

## Package Configuration

- **pyproject.toml** - Modern Python packaging (PEP 517/518)
- **Airflow provider discovery** via entry point: `apache_airflow_provider` → `get_provider_info()`
- **Version**: Manually maintained in `pyproject.toml` (current: 2.4.1)
- **Python requirement**: >= 3.11
- **Airflow requirement**: >= 2.10.0

## Release Process

Releases are automated via GitHub Actions (`.github/workflows/release.yaml`):
1. Create a GitHub release
2. Workflow builds wheel and source distribution
3. Publishes to PyPI using trusted publishing (no token needed)

Manual version bump required in `pyproject.toml` before release.

## Important Notes

- Tests are excluded from package distribution via `[tool.setuptools.packages.find]` exclude pattern
- User-Agent header includes package version for API request tracking
- Default connection ID is `"rudderstack_default"` but can be overridden
- Sync types for RETL: `"incremental"` or `"full"` (None = let backend decide)
