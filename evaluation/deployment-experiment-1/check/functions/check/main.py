import os.path
import time
from typing import Any, Dict, Optional

import boto3
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser


def handleDataManually(dataPath: str, bucket: str, objectName: str, aws_access_key_id: str, aws_secret_access_key: str):
    try:
        s3 = boto3.client(aws_access_key_id, aws_secret_access_key, "s3")
        with open(dataPath, "wb") as f:
            s3.download_fileobj(bucket, object, f)
    except Exception as e:
        print(f"an error occurred while trying to download object {objectName} from bucket {bucket}: {e}")


def handler(dataPath: Optional[str], functionInput: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    :param dataPath:
        In case of pre-fetching, the data is first downloaded to a file.
        This parameter holds the path to the file containing the pre-fetched data.
        Without pre-fetching, it is `None`.
    :param functionInput:
        If this function expects an input when being invoked, this parameter holds all input arguments.
        If it doesn't expect an input (or no inputs are passed when calling it), the parameter is `None`.
    :return:
        By returning a dictionary, this function can pass arguments to the next function in the workflow.
        It will automatically be passed onto the next step by the choreography middleware.
        If this function doesn't return anything, the next step will see `functionInput` as `None`.
    """

    time_start = int(time.time()*1000)
    print(f"start time: {time_start}")

    if functionInput is None:
        print("functionInput is None")
    else:
        print("functionInput is not None")
        print(type(functionInput))
        import json
        #print(json.dumps(functionInput))

    #print(f"handler got input {functionInput}")

    experiment = functionInput["experiment"]
    functionInput = functionInput["input"]

    print("got experiment data")
    print(experiment)

    # check if the file exists => if not, we're in the use case without pre-fetching and have to download it ourselves
    if dataPath is None or not os.path.exists(dataPath):
        print("trying to download data manually")
        dataPath = dataPath if dataPath is not None else "/tmp/test.pdf"
        bucket = functionInput["bucket"]
        objectName = functionInput["objectName"]
        aws_access_key_id = functionInput["aws_access_key_id"]
        aws_secret_access_key = functionInput["aws_secret_access_key"]
        handleDataManually(dataPath, bucket, objectName, aws_access_key_id, aws_secret_access_key)

    # parse the PDF
    with open(dataPath, "rb") as f:
        try:
            print("could open PDF, trying to parse ...")
            parser = PDFParser(f)
            document = PDFDocument(parser)
            print(f"parsed document: {document.info}")
        except Exception as e:
            print(f"something went wrong while trying to parse PDF: {e}")

    # update total workflow duration
    time_end = int(time.time()*1000)
    td: int = time_end - time_start
    print(f"total time for this function only: {td}")
    ted = experiment.get("totalExecutionDuration")
    if ted is None:
        print("setting totalExecutionDuration to 0")
        ted = 0
    ted += td
    print(f"current totalExecutionDuration {ted}")

    # input for next function
    fI = {
        "experiment": {
            "dataCollectorUrl": experiment["dataCollectorUrl"],
            "tableName": experiment["tableName"],
            "totalExecutionDuration": ted,
            "timeStartMilli": experiment["timeStartMilli"]
        },
        # for the baseline case
        "input": functionInput
    }
    print("user handler returning functionInput {fI}")
    return fI
