# Jobber

[![Run all tests](https://github.com/suiseriff/jobber/actions/workflows/pr_tests.yml/badge.svg)](https://github.com/suiseriff/jobber/actions/workflows/pr_tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

**Jobber** is a powerful and flexible framework for scheduling and managing background tasks in Python. It offers a versatile architecture for defining, executing, and storing jobs, supporting a variety of execution engines, middleware, and serialization formats.

Jobber is a robust solution that was built from the ground up to handle asynchronous tasks. It offers a clean and modern API that is inspired by leading frameworks.

## Features

- **Cron**: Jobber supports cron expressions with an optional seventh field for seconds (`* * * * * * *`) to schedule jobs with high precision and for high-frequency tasks.
- **Context Dependency Injection**: Simplify your job by automatically injecting dependencies from a shared context.
- **Lifespan Events**: Effectively manage resources with startup and shutdown events. These are useful for initializing and cleaning up resources, such as database connections, or sending a notification when the application starts or stops.
- **Middleware Support**: Jobber offers a flexible middleware processing pipeline that allows you to add custom logic before or after a job is executed. This can be useful for things like logging, authentication, or performance monitoring. The API is similar to that of frameworks like Starlette and aiogram, providing a powerful tool for customizing job execution.
- **Flexible Exception Handling**: Avoid application crashes due to a single failed job. Create custom exception handlers for various error types, enabling more detailed and flexible error handling strategies. You can record the error, try the task again, or notify the developers.
- **Full-Time Zone Support**: Let's make time zones less of a headache. We can schedule jobs reliably for users in different regions, considering their local time zones. With Jobber, it's easy to schedule jobs in any time zone and convert between them.

---

## Quick Start

Install Jobber from PyPI:
```bash
pip install jobber
```

Here is a simple example of how to schedule and run a job:

```python
import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from jobber import Jobber


UTC = ZoneInfo("UTC")
# 1. Initialize Jobber
app = Jobber(tz=UTC)


@app.task(cron="* * * * * * *")  # Runs every seconds
async def my_cron() -> None:
    print("Hello! cron running every seconds")


@app.task
def my_job(name: str) -> None:
    now = datetime.now(tz=UTC)
    print(f"Hello, {name}! job running at: {now!r}")


async def main() -> None:
    # 4. Run the Jobber application context
    async with app:
        run_next_day = datetime.now(tz=UTC) + timedelta(days=1)
        job_at = await my_job.schedule("Connor").at(run_next_day)
        job_delay = await my_job.schedule("Sara").delay(20)

        await job_at.wait()
        await job_delay.wait()

        # Or
        # It is blocked indefinitely because the cron has infinite planning.
        await app.wait_all()


if __name__ == "__main__":
    asyncio.run(main())
```
