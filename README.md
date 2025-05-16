# WideSky
## Description
A containerised Python based listener that ingests the Bluesky firehose and exports the processed data to a Postgres database to allow for easy collection of samples from BlueSky for research and wider purposes.

## Statement of Need
Bluesky is an up and coming social media site based on the AT protocol, which may merit further study. However the ATProtocol can be difficult to interpret and code around, therefore this application is designed to ease the barrier of entry for bulk collection and 

## How to use

1. Install Docker
2. Navigate to the root folder of the repository
3. Run '''docker compose up'''
4. Connect via your preferred method to the PostgreSQL database hosted locally at port 5432

*Please note:* that the bind mounted volumes are difficult to delete, due to security features within Docker. 
You will need to use a command such as `docker exec --privileged --user root <CONTAINER_ID> chown -R "$(id -u):$(id -g)" <TARGET_DIR>`, more details [here](https://stackoverflow.com/questions/42423999/cant-delete-file-created-via-docker).

## (Planned) Features
### Current Features
* Async functionality
* Exponential backoff for reconnections to firehose and reattempts for plc.directory
* Rotating Logging bind mounted to a widesky/logs folder
* Async workers for processing and batching to Postgres
* Batched Postgres saving

### To-do

* Implement graph.list post type
* Implement embed types
    * images#main
    * selectionQuote
    * secret
    * Others I have not seen?
* Improve error handling
* Add testing
* Capture PostgreSQL logs in logs/postgres
* Add webserver with metrics and ability to configure capture protocols
* Integrate with a crawler to reach back for full activity records of active users where not present already in data
* Add option to prevent HTTPX logging clogging up the logs
* Provide

## How to Contribute
Please raise an issue if you see promise in this project, or if there's an issue outstanding feel free to contribute a merge request.

## Credits
Thanks particularly to David Peck whose work I have captured in the firehose_utils.py file, who implemented a lovely decoding of the CBOR protocol. Please see his work here: https://gist.github.com/davepeck/8ada49d42d44a5632b540a4093225719 and https://github.com/davepeck.

## How to cite
A technical paper will be released soon, for now please mention the github repository and in academic works please mention my ORCID (https://orcid.org/0009-0000-1581-4021).

## License
This work is licensed under the LGPL-3.0.