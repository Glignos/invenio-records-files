# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Implementention of various utility functions."""

from __future__ import absolute_import, print_function

from flask import abort, request
from invenio_files_rest.models import ObjectVersion
from invenio_files_rest.views import ObjectResource
from invenio_records.errors import MissingModelError


def sorted_files_from_bucket(bucket, keys=None):
    """Return files from bucket sorted by given keys.

    :param bucket: :class:`~invenio_files_rest.models.Bucket` containing the
        files.
    :param keys: Keys order to be used.
    :returns: Sorted list of bucket items.
    """
    keys = keys or []
    total = len(keys)
    sortby = dict(zip(keys, range(total)))
    values = ObjectVersion.get_by_bucket(bucket).all()
    return sorted(values, key=lambda x: sortby.get(x.key, total))


def record_file_factory(pid, record, filename):
    """Get file from a record.

    :param pid: Not used. It keeps the function signature.
    :param record: Record which contains the files.
    :param filename: Name of the file to be returned.
    :returns: File object or ``None`` if not found.
    """
    try:
        if not (hasattr(record, 'files') and record.files):
            return None
    except MissingModelError:
        return None

    try:
        return record.files[filename]
    except KeyError:
        return None


def file_download_ui(pid, record, _record_file_factory=None, **kwargs):
    """File download view for a given record.

    Plug this method into your ``RECORDS_UI_ENDPOINTS`` configuration:

    .. code-block:: python

        RECORDS_UI_ENDPOINTS = dict(
            recid=dict(
                # ...
                route='/records/<pid_value/files/<filename>',
                view_imp='invenio_records_files.utils:file_download_ui',
                record_class='invenio_records_files.api:Record',
            )
        )

    If ``download`` is passed as a querystring argument, the file is sent as an
    attachment.

    :param pid: The :class:`invenio_pidstore.models.PersistentIdentifier`
        instance.
    :param record: The record metadata.
    """
    _record_file_factory = _record_file_factory or record_file_factory
    # Extract file from record.
    fileobj = _record_file_factory(
        pid, record, kwargs.get('filename')
    )

    if not fileobj:
        abort(404)

    obj = fileobj.obj

    # Check permissions
    ObjectResource.check_object_permission(obj)

    # Send file.
    return ObjectResource.send_object(
        obj.bucket, obj,
        expected_chksum=fileobj.get('checksum'),
        logger_data={
            'bucket_id': obj.bucket_id,
            'pid_type': pid.pid_type,
            'pid_value': pid.pid_value,
        },
        as_attachment=('download' in request.args)
    )


def flush_record_metadata_with_new_file(obj_uuid, **kwargs):
    from invenio_records_files.models import RecordsBuckets
    from invenio_records.models import RecordMetadata
    from invenio_db import db
    from sqlalchemy.orm.attributes import flag_modified

    # Expire all previous DB objects from the session to fetch the latest ones.
    db.session.expire_all()
    # Locking the DB object to guarantee the validity
    # of the following operation
    obj = ObjectVersion.query.filter_by(id=obj_uuid).with_for_update().one()
    if not obj.is_head:
        return False
    record_bucket =\
        RecordsBuckets.query.filter_by(bucket_id=obj.bucket_id).first()

    record = RecordMetadata.query.filter_by(
        id=record_bucket.record_id).with_for_update().one()
    keys_to_copy = ['key', 'version_id', 'bucket_id', 'file_id']
    object_version_metadata =\
        {each: getattr(obj, each) for each in keys_to_copy if getattr(obj, each, '')}

    if obj.file_id:
        file_ = obj.file
        keys_to_copy = ['checksum', 'size']
        file_metadata =\
            {each: getattr(file_, each) for each in keys_to_copy if getattr(
                file_, each, '')}
    else:
        file_metadata = {}

    new_metadata_dict = dict(object_version_metadata, **file_metadata)

    keys_to_be_translated = [('bucket_id', 'bucket')]
    for each in keys_to_be_translated:
        new_metadata_dict[each[1]] = new_metadata_dict.pop(each[0])

    is_update = False

    if '_files' in record.json:
        for existing_metadata_object in record.json['_files']:
            if existing_metadata_object['key'] == obj.key:
                is_update = True
                existing_metadata_object = new_metadata_dict
        if not is_update:
            record.json['_files'].append(new_metadata_dict)
    else:
        record.json.update({'_files': [new_metadata_dict]})
    flag_modified(record, 'json')
    db.session.merge(record)
    db.session.commit()
    return True
