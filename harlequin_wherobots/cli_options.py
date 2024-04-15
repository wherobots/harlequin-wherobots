from harlequin.options import TextOption, SelectOption
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime

token = TextOption(
    name="token",
    short_decls=["-t"],
    description="The token to use for authentication.",
)

api_key = TextOption(
    name="api-key",
    short_decls=["-k"],
    description="The API key to use for authentication.",
)

runtime = SelectOption(
    name="runtime",
    short_decls=["-r"],
    description="The Wherobots runtime to provision.",
    choices=list(Runtime.__members__.keys()),
)

region = SelectOption(
    name="region",
    short_decls=["-R"],
    description="The Wherobots region to launch into.",
    choices=list(Region.__members__.keys()),
)

ws_url = TextOption(
    name="ws-url",
    description="Direct SQL Session URL to connect to.",
)

WHEROBOTS_ADAPTER_OPTIONS = [
    token,
    api_key,
    runtime,
    region,
    ws_url,
]
