import io
import json
import logging
import sys
import time
from fdk import response
import oci

def callRatdDataflowApp(ctx, data: io.BytesIO = None):
    name = "ratdDataFlowTriggerFunction"

    try:
        pem_prefix = '-----BEGIN PRIVATE KEY-----\n'
        pem_suffix = '\n-----END PRIVATE KEY-----'
        key = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCweVl9tnJavSioO1UliKtl8qf69J52EJrDHITNbkDi4PD9IIOnntBiSmjxqXlfbT4XICRTbT4reuIaTWQQFkLh7AX1d+MPYEKbjgJUmsS0XOcFZImv2l07jKQGpnNg8ybH59D9/1v3Lpe4tP7kkqTCqpUPApuEtheUuLornTN+l80HHkhlAxQKmwhs6fzUumK6hK4QLejarUDeVUrBjroB6CGPXDMNfvb0nA5ih/50InWzV3eVeyH1x0erajL7W7pEnHshiLqTGYWbxC08EWtgzSqwsWinmx+rY/Fo06MWuHwwxmb3z8Ak4HKqVPTQkoZYzmWj48oXan03BY+AiHavAgMBAAECggEARrYZGbpFT/6DkAVWNNfydcMpc/EYnY5BtPR0ciw/a6leZs7kcgG81eWi71JNA+OuAW4roBIh2yI9/vQLqDaDTitYp+cF4F9d0R6x6FyrfOnM1+hVE1WYDghooRGJIcvMOkW1BFGR9BWDTcuYZtYrlqTrXTxaPG8KO9lZH6i5vXtBs/T18VfYz5ekGEAAg2CUvqriFGnlLZCJ1LhwYTqRdMuwf6XXCs52+miZHJn7AMYji14YzTw0sSmPdm0enFIAVbFwynusjsTRHf3NL6UhcLLA09FTvvT6e0KAITEP/VAeYKH9SQ2QW5U5m7UF0mDXPEya4SlX1tUMHcCeVrAeIQKBgQD3DUnR0oXCiAkGJhzOwhxK6bw74gqz6E1gcIfxjd/o+uA3ANYEQmwj47eoaNx0sbdQMHdI2p9KvcdUhJ6rdQPfjShifKh9Xaoka7cuqKTVMKvzTxs/lEy3ccL05BeVnYMjD3jdJw7k0XQ7J+tBFJgE4VeRFooNBYZIx2d6tbVblQKBgQC23aZaW0tsWAXTe+lJpePGeuGhGQCuV6p6t/Kn1n7xohn/RPUYrU5QHQYOL8O3+vMeKmF8ucvLi8cC8cus3h09Ll6z9eb+mTWDPd2Lwu+jHczlHcr2pwzN18gaoBjjLhqF2YKXZRmsP4UfcMz3tTTdcafQypvnGrwM8jQXwzxYMwKBgQC60QufZQjM/72DLtLd7p8ibvludxIM1X+di7rhCJ3nOb7PGQy9j9TiltJMwW7jt3edZejt6JRIGpZe7SJnGUdihwWg5A8tLeT5QZL174UlyXZduNYsD+KrXZVFRi4nb0K5AnwtD9oNYe34xcj6H66NEjH7fwXJrwHKiy9O9ZU8uQKBgD/IYfzEOTOKJEYWw1Ev7pnNRKPXP7iP1WPGg3ntRAvuGZlDKSY5VMZ2ySTrnh2vB1uvNp+1gpL1py2svvkF5Dbx1JB6pd6J+/NSAdN84+8GNvB3itKrg7jMmfxHeUbMTu3+5yD9X44H/dvwkV2ZM95FhV47PVPHrG3rkSX0sDinAoGASQprbY27kuDHax96JuZIi+zvNeXqkdDwJ4QGAI0JZT/a/KULQAhk8HSjSHoBtqFSWtMmC0OnqCe6Y3husn1EBG6nkA001PBZYpeApYyTSe+/aZGknZonOgxjPy96tiEAwigfA1GyFTii60iHY9HigYyomheURNq0oF71BZ91GHc="
        # The content of your private key
        key_content = '{}{}{}'.format(pem_prefix, key, pem_suffix)

        #compartment id where data flow application is created
        compartment_id = 'ocid1.compartment.oc1..aaaaaaaaim5pavnzdrxm2nylvxgpuj4o6m3lfe3vjua2h3fatae6a3kdhwfq'

        #config file details for the user
        config_with_key_content = {
            "user": 'ocid1.user.oc1..aaaaaaaaeks5zr4ln34774kkr36cw4ci7cxqtsc6yext6dr27vrqcmeqbqrq',
            "key_content": key_content,
            "fingerprint": 'fc:ac:72:57:0b:d8:05:70:5e:db:b5:5d:f4:3a:a5:19',
            "tenancy": 'ocid1.tenancy.oc1..aaaaaaaajt4yyodu3xeqezacrjhxddbkg2bbnx4xhvtbdbsktd66ycrmcntq',
            "region": 'us-phoenix-1'
        }
        #config = oci.config.from_file()
        client = oci.data_flow.DataFlowClient(config_with_key_content)

        print("Creating the Data Flow Run")

        #Creating the data flow run for ratd_data_flow application
        create_run_details = oci.data_flow.models.CreateRunDetails(
            compartment_id=compartment_id,
            application_id="ocid1.dataflowapplication.oc1.phx.anyhqljspwxjxiqahyue42g74gaxuzspp4l7gly3erdoetwos2r4trrefata",
            display_name="ratd_data_flow",
        )

        #Call data flow function to invoke trade processing logic
        run = client.create_run(create_run_details=create_run_details)

        #200 status code - Successfully created data flow run
        if run.status != 200:
            print("Failed to create Data Flow Run")
            print(run.data)
            sys.exit(1)
        else:
            print("Data Flow Run ID is " + run.data.id)

        # Wait for it to complete.
        probe_count = 0
        while probe_count < 2: # 15*2 = 30sec
            print("Waiting for the Run to finish")
            time.sleep(15) #15secs
            probe_count += 1
            probe = client.get_run(run_id=run.data.id)

            if probe.status != 200:
                print("Failed to load Run information")
                print(probe.data)
                sys.exit(1)
            print("Run is in state " + probe.data.lifecycle_state)
            if (
                probe.data.lifecycle_state == "ACCEPTED"
                or probe.data.lifecycle_state == "IN_PROGRESS"
            ):
                print("Run is in progress, waiting")
            elif probe.data.lifecycle_state == "FAILED":
                print("Run failed, more information:")
                print(probe.data.lifecycle_details)
                sys.exit(1)
            elif probe.data.lifecycle_state == "SUCCEEDED":
                print("Run is finished.")
                break
            else:
                print("Unexpected state {}, stopping".format(probe.data.lifecycle_state))
                sys.exit(1)
        if probe_count >= 100:
            print("Run is taking too long, giving up.")
            sys.exit(1)

        # Print the output.
        print("Output of the Data Flow Run follows:")
        log = client.get_run_log(run_id=run.data.id, name="spark_application_stdout.log.gz")
        print(log.data.text)

    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))

    logging.getLogger().info("Inside Python data flow trigger function")
    return response.Response(
        ctx, response_data=json.dumps(
            {"message": "Hello {0}".format(name)}),
        headers={"Content-Type": "application/json"}
    )