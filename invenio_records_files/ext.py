# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Flask extension for the Invenio-Records-Files."""


from __future__ import absolute_import, print_function

from invenio_files_rest.signals import file_deleted, file_uploaded

from invenio_records_files import config
from invenio_records_files.utils import flush_record_metadata_with_new_file

class InvenioRecordsFiles(object):
    """Invenio-Records-Files extension."""

    def __init__(self, app=None, **kwargs):
        """Extension initialization."""
        if app:
            self.init_app(app, **kwargs)

    def init_app(self, app):
        """Flask application initialization."""
        self.init_config(app)
        self.register_signals(app)
        app.extensions['invenio-records-files'] = self

    def init_config(self, app):
        """Initialize configuration."""
        for k in dir(config):
            if k.startswith('RECORDS_FILES_'):
                app.config.setdefault(k, getattr(config, k))

    @staticmethod
    def register_signals(app):
        """Register Files REST signals."""
        file_deleted.connect(flush_record_metadata_with_new_file, weak=False)
        file_uploaded.connect(flush_record_metadata_with_new_file, weak=False)
