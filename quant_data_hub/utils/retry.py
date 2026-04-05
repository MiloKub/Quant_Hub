# quant_data_hub/utils/retry.py
"""Retry decorator for API calls using tenacity."""

from tenacity import retry, stop_after_attempt, wait_exponential_jitter


retry_on_failure = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(max=60),
    reraise=True
)