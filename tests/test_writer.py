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
import pathlib
import shutil
import unittest
from tempfile import TemporaryDirectory

from lsst.daf.butler import Butler
from lsst.queued_butler_writer.butler import handle_prompt_processing_completion
from lsst.queued_butler_writer.messages import PromptProcessingOutputEvent


class TestButlerWrite(unittest.TestCase):
    def test_write(self):
        artifact_directory = os.path.join(self._get_data_directory(), "exported_artifacts")
        with TemporaryDirectory() as butler_tempdir, TemporaryDirectory() as artifact_tempdir:
            # As part of the ingestion process the original files are deleted,
            # so make a copy.
            shutil.copytree(artifact_directory, artifact_tempdir, dirs_exist_ok=True)

            Butler.makeRepo(butler_tempdir)
            butler = Butler.from_config(butler_tempdir, writeable=True)
            messages = [self._load_message(filename) for filename in ["message1.json", "message2.json"]]
            handle_prompt_processing_completion(butler, messages, artifact_tempdir)

            # Make sure data was ingested into the target Butler.
            self.assertEqual(
                butler.get("dt1", {"instrument": "Cam1", "detector": 1}, collections="Cam1/run"), 1
            )
            self.assertEqual(
                butler.get("dt2", {"instrument": "Cam1", "detector": 1}, collections="Cam1/run"), 2
            )
            self.assertEqual(
                butler.get("dt1", {"instrument": "Cam1", "detector": 2}, collections="Cam1/run"), 3
            )

            # Make sure that data was deleted out of the source directory.
            self.assertEqual(list(pathlib.Path(artifact_tempdir).rglob("*.json")), [])

    def _get_data_directory(self) -> str:
        test_directory = os.path.abspath(os.path.dirname(__file__))
        return os.path.join(test_directory, "data")

    def _load_message(self, filename: str) -> PromptProcessingOutputEvent:
        path = os.path.join(self._get_data_directory(), filename)
        with open(path) as fh:
            json = fh.read()
            return PromptProcessingOutputEvent.model_validate_json(json)
