# tap_pipefy
A [Singer.io](https://singer.io) tap for [Pipefy](https://www.pipefy.com/)


# tap-autopilot

[![CircleCI](https://circleci.com/gh/singer-io/tap-autopilot.svg?style=svg)](https://circleci.com/gh/singer-io/tap-autopilot)

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) for [Pipefy](https://www.pipefy.com/).

This tap:
- Pulls data from [Pipefy's API](https://pipefy.docs.apiary.io/) for a given `organization_id`
- Extracts the following resources from Pipefy
  - [Members](https://pipefy.docs.apiary.io/#reference/0/show-organization)
  - [Pipes](http://docs.pipefypipe.apiary.io/#reference/0/list-pipes)
  	This includes all the pipe phases and their fields.
  - [Cards](https://pipefypipe.docs.apiary.io/#reference/0/list-cards)
  - [Tables](http://docs.pipefydatabase.apiary.io/#reference/0/list-tables)
  	This includes all table records for each of the tables
- Outputs the schema for each resource. The tap will dynamically generate a schema for each of the tables. The `stream` name of each table is `table_<table id>` where `<table id>` is the unique table identifier assigned by Pipefy. You can see this ID in the UI by going to the table object and extracting the ID from the URL. For example, the `table_id` is g3TOB3hc in the following URL https://app.pipefy.com/database_v2/tables/g3TOB3hc-test-table-2 
- Sync all objects (full replication, incremental sync is not supported)

## Quick start

1. Install

    ```bash
    > pip install tap-pipefy
    ```

2. Get your Pipefy Personal Access Token (API Key)

    Login to your Pipefy account, navigate to your user settings and then to the "personal access tokens" section. Generate a New Token, you'll need it for the next step. [Direct Link to Personal Access Tokens](https://app.pipefy.com/tokens)

3. Create the config file

    Create a JSON file called `config.json` containing the personal access token you just generated and your organization ID.

    ```json
    {
        "personal_access_token": "your-pipefy-personal-access-token",
        "organization_id": 123456
    }
    ```

4. Discover and Catalog

    Use the discover flag to explore the schema for each of this tap's resources

    ```bash
    > tap-autopulot --config config.json --discover
    ```

    Pipe the output of this file to a file that will serve as the catalog, where you will select which streams and properties to sync

    ```bash
    > tap-pipefy --config config.json --discover > catalog.json
    ```

    The catalog is an object with a key streams that has an array of the streams for this tap. For each stream you want to sync, add a `"selected": true` property on the stream object. Below is an example of how you would select to sync the contacts stream. This property is recursive so it will select all children. If you don't want to sync a property, you can add `"selected": false` on that property.

    ```json
            {
            "schema": {
                "properties": {...},
                "type": "object",
                "selected": true
            },
            "stream": "members",
            "tap_stream_id": "members"
        }
    ```

5. [Optional] Add additional optional config parameters

    You can include a `user_agent` key in your `config.json` to further customize the behavior of this Tap.
    - `user_agent` should be set to something that includes a contact email address should the API provider need to contact you for any reason.

    If you were to use the `user_agent`, your complete config.json should look something like this.

    ```json
    {
      "personal_access_token": "your-pipefy-personal-access-token",
      "organization_id": 123456,
      "user_agent": "My Company (+support@example.com)"
    }
    ```

7. Run the application

    `tap-pipefy` can be run with:

    ```bash
    tap-pipefy --config config.json --catalog catalog.json
    ```

---

Copyright &copy; 2017
