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
              cards(first: {page_size},
                           {after}
                           pipe_id: {pipe_id}) {{
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
                      updated_at
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
                pageInfo {{
                      endCursor
                      hasNextPage
                      hasPreviousPage
                      startCursor
                    }}
              }}
             }}
             """,
    "organization": """
                    {{
                      organization(id: {organization_id} ) {{
                        name
                        created_at
                        members {{
                          user {{
                            id
                            name
                            email
                            created_at
                            avatarUrl
                            username
                            timeZone
                            locale
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
                          description
                          icon
                          created_at
                          phases {{
                            id
                            name
                            cards_count
                            fields {{
                                id
                                type
                                required
                            }}
                          }}
                        }}
                        tables {{
                          edges {{
                            node {{
                              id
                              name
                              description
                              icon
                              authorization
                              public
                              public_form
                              table_records_count
                              url
                              table_fields {{
                                id
                                label
                                type
                                description
                                is_multiple
                                unique
                                required
                                options
                                }}
                            }}
                          }}
                        }}
                      }}
                    }}
                    """,
    "table_records": """
                {{
                  table_records(first: {page_size},
                                {after}
                                table_id: "{table_id}") {{
                    edges {{
                      cursor
                      node {{
                        id
                        title
                        url
                        created_at
                        updated_at
                        finished_at
                        due_date
                        created_by {{
                            id
                        }}
                        record_fields {{
                            filled_at
                            updated_at
                            required
                            name
                            value
                            field {{
                                id
                                type
                            }}
                        }}
                      }}
                    }}
                    pageInfo {{
                      endCursor
                      hasNextPage
                      hasPreviousPage
                      startCursor
                    }}
                  }}
                }}
                 """
}

MAX_PAGE_SIZE = 50

NUMERIC_TYPES = set("currency number".split())
INTEGER_TYPES = set("id".split())
DATE_TYPES = set("date datetime due_date".split())
STRING_TYPES = set(("cnpj cpf email phone radio_horizontal radio_vertical "
                    "select short_text statement time").split())


CONFIG = {
    "page_size": 5
}

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
    """ Convert date string to UTC and RFC3339 format
    """
    if date:
        return singer.utils.strftime(pendulum.parse(date).in_timezone("UTC"))


def transform_datetimes_hook(data, typ, schema):
    """ Transform datetime fields to UTC and RFC3339 format
    """
    if typ in ["string"] and schema.get("format", "") == "date-time":
        data = format_date(data)
    return data


def get_organization(organization_id):
    """ Query API and get info for the organization_id
        Response includes pipes, phases, tables, members
    """
    params = {"organization_id": organization_id}
    query = get_query("organization", params)
    resp = request(BASE_URL, query)
    data = resp.get("data", None)
    return data.get("organization", {})


def get_after(end_cursor):
    """ Get the "after" portion of the pagination query
    """
    return 'after: "{}", '.format(end_cursor) if end_cursor else ""


def get_cards(pipe_id, end_cursor=None):
    """ Query API and get cards for the pipe_id
    """
    has_next_page = True

    while has_next_page:
        params = {
            "pipe_id": pipe_id,
            "page_size": max(1, min(CONFIG["page_size"], MAX_PAGE_SIZE)),
            "after": get_after(end_cursor)
        }

        query = get_query("cards", params)
        resp = request(BASE_URL, query)
        data = resp.get("data", {})
        cards = data.get("cards", {})

        page_info = cards.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        end_cursor = page_info.get("endCursor", "")

        for card in cards.get("edges", []):
            yield card["node"]


def process_table_record(record):
    record["created_by_id"] = record.pop("created_by", {}).pop("id", None)
    record_fields = record.pop("record_fields", [])

    for field in record_fields:
        field_dict = field.pop("field", {})
        field["id"] = field_dict.get("id", "")
        field["type"] = field_dict.get("type", "")

    record["record_fields"] = record_fields
    return record


def get_table_records(table_id, end_cursor=None):
    """ Query API and get table_records for the table_id
    """
    has_next_page = True

    while has_next_page:
        params = {
            "table_id": table_id,
            "page_size": max(1, min(CONFIG["page_size"], MAX_PAGE_SIZE)),
            "after": get_after(end_cursor)
        }

        query = get_query("table_records", params)
        resp = request(BASE_URL, query)
        data = resp.get("data", {})
        table_records = data.get("table_records", {})

        page_info = table_records.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        end_cursor = page_info.get("endCursor", "")

        for record in table_records.get("edges", []):
            record["node"]["table_id"] = table_id
            yield process_table_record(record["node"])


def test_api_connection():
    """ Send 'organization' query to the API to test connection
    """
    LOGGER.info("Testing API connection. Issuing 'organization' query")

    org = get_organization(CONFIG["organization_id"])

    if org:
        LOGGER.info("API connection successful")
        LOGGER.info("organization_id: %s, name: %s",
                    CONFIG["organization_id"], org["name"])
    else:
        LOGGER.critical(
            "API connection failed. Unable to find data for organization: %s",
            CONFIG["organization_id"]
        )
        sys.exit(-1)


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
# TODO: Convert to class

Stream = collections.namedtuple(
    "Stream",
    "tap_stream_id stream primary_keys discovered_schema catalog_schema"
)

STREAMS = [
    Stream("members", "members", "id".split(), {}, {}),
    Stream("pipes", "pipes", "id".split(), {}, {}),
    Stream("pipe_phases", "pipe_phases", "id".split(), {}, {}),
    Stream("cards", "cards", "id".split(), {}, {}),
    Stream("tables", "tables", "id".split(), {}, {}),
    Stream("table_records", "table_records", "id".split(), {}, {})
]

load_discovered_schemas(STREAMS)


def load_catalog_schemas(catalog):
    """ Updates STREAMS.catalog_schema with the schema read from the catalog
    """
    for stream in STREAMS:
        catalog_stream = catalog.get_stream(stream.tap_stream_id)
        stream.catalog_schema.update(catalog_stream.schema.to_dict())


def get_schema_for_table(table):
    schema = {"type": "object", "properties": {}, "required": []}
    table_fields = table.get("table_fields", [])

    record_id_field = {
        "id": "_record_id",
        "type": "integer",
        "required": True,
        "is_multiple": False
    }

    table_fields.append(record_id_field)

    for field in table_fields:
        property_schema = {"inclusion": "automatic"}
        property_schema['type'] = []

        if field["required"]:
            schema["required"].append(field["id"])
        else:
            property_schema['type'].append("null")

        if field["type"] in (STRING_TYPES | DATE_TYPES) \
                or field["is_multiple"]:
            property_schema['type'].append("string")

        if field["type"] in DATE_TYPES:
            property_schema["format"] = "date-time"

        if field["type"] in NUMERIC_TYPES:
            property_schema["type"].append("number")

        if field["type"] in INTEGER_TYPES or field["type"] == "integer":
            property_schema["type"].append("integer")

        schema["properties"][field["id"]] = property_schema

    return schema


def get_dynamic_schemas():
    schemas = []
    org = get_organization(CONFIG["organization_id"])
    tables = org.pop("tables", [])
    tables = [tab["node"] for tab in tables.get("edges", [])]

    for table in tables:
        schema = {}
        schema["stream"] = "table_{}".format(table["id"])
        schema["tap_stream_id"] = schema["stream"]
        schema["key_properties"] = ["_record_id"]
        schema["schema"] = get_schema_for_table(table)
        schemas.append(schema)

    return schemas


def discover_schemas():
    """ Generate a list of streams supported by the tap
    """
    schemas = []
    for stream in STREAMS:  # Static schenas
        schema = {
            'tap_stream_id': stream.tap_stream_id,
            'stream': stream.stream,
            'schema': stream.discovered_schema,
            'key_properties': stream.primary_keys
        }
        schemas.append(schema)
        schemas.extend(get_dynamic_schemas())

    return {'streams': schemas}


def get_stream(tap_stream_id):
    """ Return stream matching the tap_stream_id
    """
    stream = [s for s in STREAMS if s.tap_stream_id == tap_stream_id]
    return next(iter(stream), None)


def write_catalog_schema(stream):
    """ Output SCHEMA message for the stream
    """
    if stream:
        singer.write_schema(
            stream.tap_stream_id,
            stream.catalog_schema,
            stream.primary_keys
        )


def write_members(members):
    """ Process members array and output SCHEMA and RECORD messages
    """
    members_stream = get_stream("members")
    write_catalog_schema(members_stream)

    for member in members:
        user = member.pop("user")
        member.update(user)

        with Transformer(pre_hook=transform_datetimes_hook) as xform:
            member = xform.transform(member, members_stream.catalog_schema)
            singer.write_record("members", member)


def write_pipes_and_cards(pipes):
    """ Process pipes array and output SCHEMA and RECORD messages
    """
    pipes_stream = get_stream("pipes")
    cards_stream = get_stream("cards")

    write_catalog_schema(pipes_stream)
    write_catalog_schema(cards_stream)

    for pipe in pipes:
        with Transformer(pre_hook=transform_datetimes_hook) as xform:
            pipe = xform.transform(pipe, pipes_stream.catalog_schema)
            singer.write_record("pipes", pipe)

            for card in get_cards(pipe["id"]):
                card["pipe_id"] = pipe["id"]
                card = xform.transform(card, cards_stream.catalog_schema)
                singer.write_record("cards", card)


def write_tables_and_records(tables):
    tables = [tab["node"] for tab in tables.get("edges", [])]

    tables_stream = get_stream("tables")
    table_records_stream = get_stream("table_records")

    write_catalog_schema(tables_stream)
    write_catalog_schema(table_records_stream)

    with Transformer(pre_hook=transform_datetimes_hook) as xform:
        for table in tables:
            table = xform.transform(table, tables_stream.catalog_schema)
            singer.write_record("tables", table)

            for table_record in get_table_records(table["id"]):
                table_record = xform.transform(
                    table_record, table_records_stream.catalog_schema)
                singer.write_record("table_records", table_record)


def sync_organization(organization_id):
    """ Sync data for an organization.
        Data includes pipes + phases and tables
    """
    org = get_organization(organization_id)
    members = org.pop("members", [])
    pipes = org.pop("pipes", [])
    tables = org.pop("tables", [])

    write_members(members)
    write_pipes_and_cards(pipes)
    write_tables_and_records(tables)


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
