import collections
import json
import os.path
import sys

import pendulum
import requests

import singer
from singer import utils
from singer import Transformer

REQUIRED_CONFIG_KEYS = ("organization_id personal_access_token").split()
SENSITIVE_CONFIG_KEYS = "personal_access_token".split()  # Won't be logged

BASE_URL = "https://app.pipefy.com/queries"

# Brackets in queries need to be escaped
# Field names are still enclosed in single brackets {}
# For other brackets use double brackets {{ }}

QUERIES = {
    "me": """
          {{
             me {{
                id
                name
                username
                email
                avatarUrl
                created_at
                locale
                timeZone
              }}
            }}
            """,
    "cards": """
             {{
              cards(pipe_id: {pipe_id}) {{
                edges {{
                  node {{
                    id
                    title
                    assignees {{
                      id
                    }}
                    comments {{
                      text
                    }}
                    comments_count
                    current_phase {{
                      name
                    }}
                    done
                    due_date
                    fields {{
                      name
                      value
                    }}
                    labels {{
                      name
                    }}
                    phases_history {{
                      phase {{
                        name
                      }}
                      firstTimeIn
                      lastTimeOut
                    }}
                    url
                  }}
                }}
              }}
             }}
             """,
    "organizations": """
                    {{
                      organizations(ids: [ {organization_id} ]) {{
                        name
                        created_at
                        members {{
                          user {{
                            id
                            name
                          }}
                          role_name
                        }}
                        only_admin_can_create_pipes
                        only_admin_can_invite_users
                        automations {{
                          id
                        }}
                        pipes(include_publics: true) {{
                          id
                          name
                          phases {{
                            id
                            name
                            cards_count
                            fields {{
                                id
                            }}
                          }}
                        }}
                        tables {{
                          edges {{
                            node {{
                              id
                              name
                            }}
                          }}
                        }}
                      }}
                    }}
"""
}

CONFIG = {}

STATE = {}

LOGGER = singer.get_logger()

SESSION = requests.session()


def get_query(key, params=None):
    params = params or {}
    return QUERIES[key].format(**params)


def request(url, query):
    """ Issue http POST request to url, return json response and handle errors
    """
    resp_json = {}
    headers = {
        "Authorization": "Bearer {}".format(CONFIG['personal_access_token']),
        "Accept": "application/json"
    }

    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    data = {"query": query}

    req = requests.Request('POST', url, headers=headers, data=data).prepare()
    LOGGER.info("%s %s \n     QUERY: %s", req.method, req.url, query)

    try:
        resp = SESSION.send(req, timeout=10)

        if resp:
            resp_json = resp.json()
            return resp_json

        if resp.status_code >= 400:
            resp.raise_for_status()

        if resp is None:
            LOGGER.error("Blank response from %s", url)

    except (requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as exc:
        LOGGER.error("Request: %s : e: %s", req.url, exc)

    return resp_json


def log_config_keys():
    """ Prints config keys to the log masking the sensitive ones
    """
    for key, value in CONFIG.items():
        if key in SENSITIVE_CONFIG_KEYS:
            value = "*" * len(value)

        LOGGER.info("CONFIG: %s = %s", key, value)


def get_abs_path(path):
    """ Get absolute path for the current file
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(stream):
    """ Load schema for the stream
    """
    return utils.load_json(get_abs_path("schemas/{}.json".format(stream)))


def format_date(date):
    if date:
        return singer.utils.strftime(pendulum.parse(date).in_timezone("UTC"))


def transform_datetimes_hook(data, typ, schema):
    """ Transform datetime to UTC time zone
    """
    if typ in ["string"] and schema.get("format", "") == "date-time":
        data = format_date(data)
    return data


def get_organization(organization_id):
    params = {"organization_id": organization_id}
    query = get_query("organizations", params)
    resp = request(BASE_URL, query)
    data = resp.get("data", None)
    orgs = data.get("organizations", [])
    if orgs:
        return next(iter(orgs), {})


def get_cards(pipe_id):
    params = {"pipe_id": pipe_id}
    query = get_query("cards", params)
    resp = request(BASE_URL, query)
    data = resp.get("data", {})
    cards = data.get("cards", {})
    return [card["node"] for card in cards.get("edges", [])]


def test_api_connection():
    LOGGER.info("Testing API connection. Issuing 'me' query")

    query = get_query("me")
    resp_json = request(BASE_URL, query)
    data = resp_json.get("data", {})
    errors = resp_json.get("errors", {})

    if "me" in data:
        LOGGER.info("API connection successful", data)
    else:
        LOGGER.error("API connection unsuccesful")
        if errors:
            LOGGER.error("API returned: %s", errors)


def load_discovered_schema(stream):
    """ Append 'inclusion': 'automatic' property to all fields in the schema
    """
    schema = load_schema(stream.stream)
    for key in schema['properties']:
        schema['properties'][key]['inclusion'] = 'automatic'
    return schema


def load_discovered_schemas(streams):
    """ Load default schemas for all streams
    """
    for stream in streams:
        LOGGER.info('Loading schema for %s', stream.tap_stream_id)
        stream.discovered_schema.update(load_discovered_schema(stream))


# Configure available streams

Stream = collections.namedtuple(
    "Stream",
    "tap_stream_id stream primary_keys discovered_schema catalog_schema"
)

STREAMS = [
    Stream("pipes", "pipes", "id".split(), {}, {}),
    Stream("pipe_phases", "pipe_phases", "id".split(), {}, {}),
    Stream("cards", "cards", "id".split(), {}, {})
]

load_discovered_schemas(STREAMS)


def load_catalog_schemas(catalog):
    """ Updates STREAMS.catalog_schema with the schema read from the catalog
    """
    for stream in STREAMS:
        catalog_stream = catalog.get_stream(stream.tap_stream_id)
        stream.catalog_schema.update(catalog_stream.schema.to_dict())


def discover_schemas():
    """ Generate a list of streams supported by the tap
    """
    schemas = []
    for stream in STREAMS:
        schema = {
            'tap_stream_id': stream.tap_stream_id,
            'stream': stream.stream,
            'schema': stream.discovered_schema
        }
        schemas.append(schema)

    return {'streams': schemas}


def get_stream(tap_stream_id):
    stream = [s for s in STREAMS if s.tap_stream_id == tap_stream_id]
    return next(iter(stream), None)


def write_catalog_schema(stream):
    if stream:
        singer.write_schema(
            stream.tap_stream_id,
            stream.catalog_schema,
            stream.primary_keys
        )


def write_pipes_phases_and_cards(pipes):
    pipes_stream = get_stream("pipes")
    pipe_phases_stream = get_stream("pipe_phases")
    cards_stream = get_stream("cards")

    write_catalog_schema(pipes_stream)
    write_catalog_schema(pipe_phases_stream)
    write_catalog_schema(cards_stream)

    for pipe in pipes:
        phases = pipe.pop("phases", [])
        cards = get_cards(pipe["id"])

        with Transformer(pre_hook=transform_datetimes_hook) as xform:
            pipe = xform.transform(pipe, pipes_stream.catalog_schema)
            singer.write_record("pipes", pipe)

            for phase in phases:
                phase["pipe_id"] = pipe["id"]
                phase = xform.transform(
                    phase, pipe_phases_stream.catalog_schema)
                singer.write_record("pipe_phases", phase)

            for card in cards:
                card["pipe_id"] = pipe["id"]
                card = xform.transform(card, cards_stream.catalog_schema)
                singer.write_record("cards", card)


def sync_organization(organization_id):
    """ Get data for an organization.
        Data includes pipes + phases and tables
    """
    org = get_organization(organization_id)
    pipes = org.pop("pipes", [])
    tables = org.pop("tables", [])

    write_pipes_phases_and_cards(pipes)


# def do_sync(state, catalog):
#     """ Sync all selected streams
#     """
#     selected_streams = get_selected_streams(STREAMS, catalog)
#     LOGGER.info("Starting Sync for %s",
#                 [s.tap_stream_id for s in selected_streams])

#     for stream in selected_streams:
#         LOGGER.info("Syncing %s", stream.tap_stream_id)

#         state = stream.sync(state, stream.tap_stream_id, catalog)

#     singer.write_state(state)


def do_discover():
    """ Output streams supported by the tap
    """
    LOGGER.info('Loading schemas')
    json.dump(discover_schemas(), sys.stdout, indent=4)


def main_impl():
    """ Main entry point
    """
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.state:
        STATE.update(args.state)
        LOGGER.info("STATE: %s", STATE)

    CONFIG.update(args.config)
    log_config_keys()

    if args.discover:
        do_discover()
        test_api_connection()
    elif args.catalog:
        load_catalog_schemas(args.catalog)

        # do_sync(STATE, args.catalog)
        sync_organization(CONFIG["organization_id"])
    else:
        LOGGER.info("No catalog was provided")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise


if __name__ == '__main__':
    main()
