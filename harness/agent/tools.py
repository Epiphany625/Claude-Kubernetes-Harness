from typing import Final
KUBEMCP_READ_TOOLS: Final = set(
        ["mcp__kubemcp__list_resources",
        "mcp__kubemcp__get_resource",
        "mcp__kubemcp__describe_resource",
        "mcp__kubemcp__get_pod_logs",
        "mcp__kubemcp__list_events",
        "mcp__kubemcp__get_top_metrics",
        "mcp__kubemcp__list_api_resources", 
        "mcp__kubemcp__get_cluster_info"
        ]
)

KUBEMCP_NO_TOOLS: Final = set([])