from typing import Any

import requests

from vnag.local import LocalTool


def fetch_html(url: str) -> str:
    """
    获取并返回指定URL的HTML内容。
    """
    try:
        response: requests.Response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        return f"Error getting HTML: {e}"


def fetch_json(url: str) -> Any:
    """
    获取并解析来自URL的JSON数据。
    """
    try:
        response: requests.Response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return f"Error getting JSON: {e}"
    except ValueError:
        return "Failed to parse JSON, the response content may not be in valid JSON format"


def check_link(url: str) -> str:
    """
    检查链接的HTTP状态。
    """
    try:
        response: requests.Response = requests.head(url, timeout=5, allow_redirects=True)
        return f"Status code: {response.status_code} {response.reason}"
    except requests.exceptions.RequestException as e:
        return f"Error checking link: {e}"


#Registration tool
fetch_html_tool: LocalTool = LocalTool(fetch_html)

fetch_json_tool: LocalTool = LocalTool(fetch_json)

check_link_tool: LocalTool = LocalTool(check_link)
