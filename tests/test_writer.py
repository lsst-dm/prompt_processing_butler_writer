# This file is part of prompt_processing_butler_writer.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
import shutil
import unittest
from uuid import UUID
from tempfile import TemporaryDirectory

from lsst.daf.butler import Butler, DatasetType
from lsst.resources import ResourcePath
from lsst.queued_butler_writer.main import MessageProcessor, ServiceConfig
from lsst.queued_butler_writer.kafka import MockKafkaConnection
from lsst.queued_butler_writer.messages import BatchIngestedEvent, DatasetBatch, PromptProcessingDayEndEvent


class TestButlerWrite(unittest.TestCase):
    def setUp(self) -> None:
        self._butler_root = self.enterContext(TemporaryDirectory())
        Butler.makeRepo(self._butler_root)
        self._butler = Butler.from_config(self._butler_root, writeable=True)

        self._output_dir = self.enterContext(TemporaryDirectory())
        mock_config = ServiceConfig(
            BUTLER_REPOSITORY="",
            KAFKA_CLUSTER="",
            KAFKA_TOPIC="",
            KAFKA_OUTPUT_TOPIC="",
            OUTPUT_DATASET_LIST_DIRECTORY=f"file://{self._output_dir}",
        )
        self._kafka_reader = MockKafkaConnection()
        self._message_processor = MessageProcessor(mock_config, self._butler, self._kafka_reader)

    def test_write(self) -> None:
        artifact_directory = os.path.join(self._get_data_directory(), "exported_artifacts")
        # Copy the files into the Butler datastore directory so they can be
        # found by ingest.
        shutil.copytree(artifact_directory, self._butler_root, dirs_exist_ok=True)

        # Register a dataset type which is used by datasets in the
        # messages, but not explicitly included there.
        # If the dataset type is already registered in the target Butler,
        # it does not have to be specified in the messages.
        self._butler.registry.registerDatasetType(
            DatasetType("dt2", ["instrument", "detector"], "int", universe=self._butler.dimensions)
        )

        messages = [self._load_message(filename) for filename in ["message1.json", "message2.json"]]
        self._kafka_reader.add_messages(messages)
        self._message_processor.process_messages()
        self.assertFalse(self._kafka_reader.has_pending_messages())
        self.assertEqual(len(self._kafka_reader.last_output), 1)
        output = BatchIngestedEvent.model_validate_json(self._kafka_reader.last_output[0])
        self.assertEqual(output.origin, "prompt_processing")
        self.assertIn(str(output.batch_id), output.batch_file)
        batch = self._read_output_batch(output)
        self.assertCountEqual(
            batch.datasets,
            [
                UUID("26c6235f-2ea3-5a15-8009-a3056e49c379"),
                UUID("07f98c57-9075-5e6c-a710-8aa4bfbe74a3"),
                UUID("58adc38b-0374-4656-9e7e-24d67792c02d"),
            ],
        )

        # Make sure data was ingested into the target Butler.
        self.assertEqual(
            self._butler.get("dt1", {"instrument": "Cam1", "detector": 1}, collections="Cam1/run"), 1
        )
        self.assertEqual(
            self._butler.get("dt2", {"instrument": "Cam1", "detector": 1}, collections="Cam1/run"), 2
        )
        self.assertEqual(
            self._butler.get("dt1", {"instrument": "Cam1", "detector": 2}, collections="Cam1/run"), 3
        )

        with self.assertLogs(level="ERROR") as logs:
            # Process a message that will trigger a
            # ConflictingDefinitionError in the Butler, and ensure that we
            # log the error but do not abort processing.
            self._kafka_reader.add_messages([self._load_message("conflicting-message.json"), *messages])
            self._message_processor.process_messages()
            self.assertFalse(self._kafka_reader.has_pending_messages())
            self.assertEqual(len(self._kafka_reader.last_output), 1)
            output = BatchIngestedEvent.model_validate_json(self._kafka_reader.last_output[0])
            batch = self._read_output_batch(output)
            self.assertCountEqual(
                batch.datasets,
                [
                    UUID("26c6235f-2ea3-5a15-8009-a3056e49c379"),
                    UUID("07f98c57-9075-5e6c-a710-8aa4bfbe74a3"),
                    UUID("58adc38b-0374-4656-9e7e-24d67792c02d"),
                ],
            )
        self.assertTrue(any("Encountered unrecoverable error" in msg for msg in logs.output))

    def test_day_end(self) -> None:
        message = self._load_message("day-end-message.json")
        self._kafka_reader.add_messages([message])
        self._message_processor.process_messages()
        self.assertFalse(self._kafka_reader.has_pending_messages())

        # The day-end message should be forwarded unchanged.
        self.assertEqual(len(self._kafka_reader.last_output), 1)
        output = PromptProcessingDayEndEvent.model_validate_json(self._kafka_reader.last_output[0])
        self.assertEqual(output.instrument, "LSSTCam")
        self.assertEqual(output.day_obs, 20240924)
        self.assertEqual(output.surveys, ["SURVEY"])
        self.assertEqual(output.version, "g12345")
        self.assertEqual(list(ResourcePath.findFileResources([self._output_dir])), [])

    def test_mixed_messages(self) -> None:
        artifact_directory = os.path.join(self._get_data_directory(), "exported_artifacts")
        shutil.copytree(artifact_directory, self._butler_root, dirs_exist_ok=True)
        self._butler.registry.registerDatasetType(
            DatasetType("dt2", ["instrument", "detector"], "int", universe=self._butler.dimensions)
        )

        messages = [
            self._load_message(filename)
            for filename in ["message1.json", "message2.json", "day-end-message.json"]
        ]
        self._kafka_reader.add_messages(messages)
        self._message_processor.process_messages()
        self.assertFalse(self._kafka_reader.has_pending_messages())

        # Should produce a BatchIngestedEvent and a day-end event.
        self.assertEqual(len(self._kafka_reader.last_output), 2)
        batch_ingested = [msg for msg in self._kafka_reader.last_output if '"type":"batch-ingested"' in msg]
        day_end = [msg for msg in self._kafka_reader.last_output if '"type":"day-end"' in msg]
        self.assertEqual(len(batch_ingested), 1)
        self.assertEqual(len(day_end), 1)

        output = BatchIngestedEvent.model_validate_json(batch_ingested[0])
        batch = self._read_output_batch(output)
        self.assertCountEqual(
            batch.datasets,
            [
                UUID("26c6235f-2ea3-5a15-8009-a3056e49c379"),
                UUID("07f98c57-9075-5e6c-a710-8aa4bfbe74a3"),
                UUID("58adc38b-0374-4656-9e7e-24d67792c02d"),
            ],
        )

        day_end_output = PromptProcessingDayEndEvent.model_validate_json(day_end[0])
        self.assertEqual(day_end_output.instrument, "LSSTCam")
        self.assertEqual(day_end_output.day_obs, 20240924)

    def test_unparseable_message(self) -> None:
        artifact_directory = os.path.join(self._get_data_directory(), "exported_artifacts")
        shutil.copytree(artifact_directory, self._butler_root, dirs_exist_ok=True)
        self._butler.registry.registerDatasetType(
            DatasetType("dt2", ["instrument", "detector"], "int", universe=self._butler.dimensions)
        )

        messages = [self._load_message(filename) for filename in ["message1.json", "message2.json"]]
        self._kafka_reader.add_messages([*messages, '{"type": "unknown-type"}'])
        with self.assertLogs(level="ERROR") as logs:
            self._message_processor.process_messages()
        self.assertFalse(self._kafka_reader.has_pending_messages())
        self.assertTrue(any("Discarding unparseable message" in msg for msg in logs.output))

        # The two valid messages should still be processed normally.
        self.assertEqual(len(self._kafka_reader.last_output), 1)
        output = BatchIngestedEvent.model_validate_json(self._kafka_reader.last_output[0])
        batch = self._read_output_batch(output)
        self.assertCountEqual(
            batch.datasets,
            [
                UUID("26c6235f-2ea3-5a15-8009-a3056e49c379"),
                UUID("07f98c57-9075-5e6c-a710-8aa4bfbe74a3"),
                UUID("58adc38b-0374-4656-9e7e-24d67792c02d"),
            ],
        )

    def _get_data_directory(self) -> str:
        test_directory = os.path.abspath(os.path.dirname(__file__))
        return os.path.join(test_directory, "data")

    def _load_message(self, filename: str) -> str:
        path = os.path.join(self._get_data_directory(), filename)
        with open(path) as fh:
            return fh.read()

    def _read_output_batch(self, message: BatchIngestedEvent) -> DatasetBatch:
        batch_json = ResourcePath(self._output_dir).join(message.batch_file).read().decode("utf-8")
        batch = DatasetBatch.model_validate_json(batch_json)
        self.assertEqual(message.batch_id, batch.batch_id)
        return batch
