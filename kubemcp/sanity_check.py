import asyncio
from mcp import Client


async def main():
    async with Client("http://127.0.0.1:8080/mcp") as client:
        result = await client.call_tool(
            "describe_resource",
            {
                "api_version": "v1",
                "kind": "Pod",
                "name": "cert-manager-5c4b7b5c7b-v6mfp",
                "namespace": "cert-manager",
            },
        )
        print(result.structured_content)


asyncio.run(main())