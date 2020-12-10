#!/usr/bin/env python3
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.pvalue import AsList
import logging
import json
import argparse


def process_artists(row, gender, area):
    """
    Processes artist PCollection with gender and area PCollections as side inputs
    The function will
    :param row: Dictionary element from beam.PCollection
    :param gender: list of gender id and name mappings
    :param area: list of area id and name mappings
    :return: tuple in the form (id, row)
    """
    reduced_row = {
        'id': row['id'],
        'artist_gid': row['gid'],
        'artist_name': row['name'],
        'area': row['area'],
        'gender': row['gender'],
    }
    if reduced_row['gender']:
        for g in gender:
            if g['id'] == reduced_row['gender']:
                reduced_row['gender'] = g['name']
    for a in area:
        if a['id'] == reduced_row['area']:
            reduced_row['area'] = a['name']
    return (reduced_row['id'], reduced_row)


def process_gender_or_area(element):
    """
    Utility function that processes text json from area.json or gender.json
    :param element: String json object that needs to be parsed
    :return: {id: int, name: string}
    """
    row = json.loads(element)
    return {
        'id': row['id'],
        'name': row['name']
    }


def process_artist_credit(element):
    """
    This function is used to decode json elements from artist_credit_name.json.
    :param element: json string element
    :return: set(artist_id, dict). Dictionary has only columns of interest preserved from the original element
    """
    row = json.loads(element)
    reduced_row = {
        'artist_credit': row['artist_credit'],
        'artist': row['artist']
    }
    return (reduced_row['artist'], reduced_row)


def process_recording(element):
    """
    This method processes json records in recording.json
    :param element: Json string object
    :return: set(artist_credit, dict). Dictionary has only columns of interest preserved from the original element
    """
    row = json.loads(element)
    reduced_row = {
        'recording_name': row['name'],
        'length': row['length'],
        'recording_gid': row['gid'],
        'video': row['video'],
        'artist_credit': row['artist_credit']
    }
    return (reduced_row['artist_credit'], reduced_row)


class UnSetCoGroup(beam.DoFn):
    def process(self, element, source, joined, exclude_join_field):
        """
        This method finalizes inner join. element is in the following form
        (key, {source:[some dict elements], joined: [some dict elements]}). In order to perform the full
        left join we need to combine columns from source with columns from joined.
        In a nutshell we are doing a cartesian product
        :param element: set containing id and the dictionary object
        :param source: key for source array in the dictionary object
        :param joined: key for joined array in the dictionary object
        :param exclude_join_field: Field that should be excluded from objects in joined array when merging with
        objects from source array
        :return: joined dictionary
        """
        _, grouped_dict = element
        sources = grouped_dict[source]
        joins = grouped_dict[joined]
        for src in sources:
            for join in joins:
                for k, v in join.items():
                    if k != exclude_join_field:
                        src[k] = v
                yield src


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dataset',
        default='musicbrainz',
        help='BigQuery dataset name'
    )
    parser.add_argument(
        '--table',
        default='recordings_by_artists_dataflow',
        help='BiqQuery table'
    )
    args, argv = parser.parse_known_args()

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    if not gcp_options.job_name:
        gcp_options.job_name = 'music-job'
    worker_options = pipeline_options.view_as(WorkerOptions)
    if not worker_options.use_public_ips:
        worker_options.use_public_ips = False

    table_spec = bigquery.TableReference(projectId=gcp_options.project,
                                         datasetId=args.dataset,
                                         tableId=args.table)
    table_schema = {
        'fields': [
            {'name': 'id', 'mode': 'NULLABLE', 'type': 'INTEGER'},
            {'name': 'artist_gid', 'mode': 'NULLABLE', 'type': 'STRING'},
            {'name': 'artist_name', 'mode': 'NULLABLE', 'type': 'STRING'},
            {'name': 'area', 'mode': 'NULLABLE', 'type': 'STRING'},
            {'name': 'gender', 'mode': 'NULLABLE', 'type': 'STRING'},
            {'name': 'artist_credit', 'mode': 'NULLABLE', 'type': 'INTEGER'},
            {'name': 'recording_name', 'mode': 'NULLABLE', 'type': 'STRING'},
            {'name': 'length', 'mode': 'NULLABLE', 'type': 'INTEGER'},
            {'name': 'recording_gid', 'mode': 'NULLABLE', 'type': 'STRING'},
            {'name': 'video', 'mode': 'NULLABLE', 'type': 'BOOLEAN'},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as pipeline:
        gender = pipeline | \
                 'Read gender' >> beam.io.ReadFromText('gs://solutions-public-assets/bqetl/gender.json') | \
                 'Process gender' >> beam.Map(process_gender_or_area)

        area = pipeline | \
               'Read area' >> beam.io.ReadFromText('gs://solutions-public-assets/bqetl/area.json') | \
               'Process area' >> beam.Map(process_gender_or_area)

        artists = pipeline | \
                  'Read Artists' >> beam.io.ReadFromText('gs://solutions-public-assets/bqetl/artist.json') | \
                  'Convert artist from json to dict' >> beam.Map(lambda e: json.loads(e)) | \
                  'Process artists' >> beam.Map(process_artists, AsList(gender), AsList(area))

        recordings = pipeline | \
                     'Read Recordings' >> beam.io.ReadFromText('gs://solutions-public-assets/bqetl/recording.json') | \
                     'Process recording' >> beam.Map(process_recording)

        artist_credit_name = pipeline | \
                             'Read Artists Credit Name' >> beam.io.ReadFromText('gs://solutions-public-assets/bqetl/artist_credit_name.json') | \
                             'Process artist credit name' >> beam.Map(process_artist_credit)

        # Joining artist and artist_credit_name
        # SELECT artist.id,
        #   artist.gid as artist_gid,
        #   artist.name as artist_name,
        #   artist.area,
        #   artist_credit_name.artist_credit
        # FROM datafusion-dataproc-tutorial.musicbrainz.artist as artist
        #  INNER JOIN datafusion-dataproc-tutorial.musicbrainz.artist_credit_name AS artist_credit_name
        #       ON artist.id = artist_credit_name.artist
        #
        joined_artist_and_artist_credit_name = ({
            'artists': artists,
            'artist_credit_name': artist_credit_name}) | \
            'Merge artist and artist_credit_name to intermitent' >> beam.CoGroupByKey() | \
            'UnSetCoGroup intermitent' >> beam.ParDo(UnSetCoGroup(),
                                                     'artists',
                                                     'artist_credit_name',
                                                     'artist') | \
            'Map artist_credit to dict element' >> beam.Map(lambda e: (e['artist_credit'], e))

        # Joining previous table with recordings
        # SELECT intermitent.id,
        #   intermitent.artist_gid,
        #   intermitent.artist_name,
        #   intermitent.area,
        #   intermitent.artist_credit,
        #   recording.recording_name,
        #   recording.length,
        #   recording.video
        # FROM datafusion-dataproc-tutorial.musicbrainz.intermitents as intermitent
        #  INNER JOIN datafusion-dataproc-tutorial.musicbrainz.recording AS recording
        #       ON intermitent.artist_credit = recording.artist_credit
        #
        joined_artist_and_artist_credit_name_and_recording = ({
            'joined_artist_and_artist_credit_name': joined_artist_and_artist_credit_name,
            'recordings': recordings}) | \
            'Merge intermitent and recording' >> beam.CoGroupByKey() | \
            'UnSetCoGroup final' >> beam.ParDo(UnSetCoGroup(),
                                               'joined_artist_and_artist_credit_name',
                                               'recordings',
                                               'artist_credit') | \
            'Write To BQ' >> beam.io.WriteToBigQuery(table_spec,
                                                     schema=table_schema,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    logging.getLogger().setLevel(logging.INFO)


main()
