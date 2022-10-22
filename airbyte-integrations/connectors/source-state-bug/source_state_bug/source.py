#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import json
import typing
from datetime import datetime
from typing import Generator

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type, AirbyteStateMessage, AirbyteStateType, AirbyteStreamState, StreamDescriptor, AirbyteStateBlob, SyncMode,
)
from airbyte_cdk.sources import Source


class SourceStateBug(Source):
    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        try:
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        streams = []

        stream_name = "Test"
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"test": {"type": "string"}}
        }

        streams.append(AirbyteStream(name=stream_name, json_schema=json_schema, supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental]))
        return AirbyteCatalog(streams=streams)

    def read(
        self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: typing.List[AirbyteStateMessage]
    ) -> Generator[AirbyteMessage, None, None]:

        stream_name = "Test"
        fail_on_number = config["fail-on-number"]

        last_number = 1 if not state else state[0].stream.stream_state.dict()['last_number']
        print(f"Last number: {last_number}")

        for i in range(last_number, 10):
            print(f"Loop variable: {i}")
            if last_number == 1 and fail_on_number == i:
                raise Exception("Stopping for expected exception")
            yield AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(stream=stream_name, data={"test": f"Number: {i}"}, emitted_at=int(datetime.now().timestamp()) * 1000),
            )
            yield AirbyteMessage(
                type=Type.STATE,
                state=AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(stream_descriptor=StreamDescriptor(name="Test"), stream_state=AirbyteStateBlob.parse_obj(
                        {"last_number": i}))
                )
            )
