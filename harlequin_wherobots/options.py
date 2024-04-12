from harlequin.options import TextOption

token = TextOption(
    name="token",
    short_decls=["-t"],
    description="The token to use for authentication.",
)

api_key = TextOption(
    name="api_key",
    short_decls=["-k"],
    description="The API key to use for authentication.",
)

ws_url = TextOption(
    name="ws_url",
    description="Direct SQL Session URL to connect to.",
)

WHEROBOTS_ADAPTER_OPTIONS = [
    token,
    api_key,
    ws_url,
]
