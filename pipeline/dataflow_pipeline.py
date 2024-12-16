
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging


class EvaluateDataQuality(beam.DoFn):
    """Custom data quality evaluation."""
    def process(self, row):
        errors = []
        # Completeness checks
        if not row.get('poster_link'):
            errors.append('poster_link is missing')
        if len(row.get('series_title', '')) > 69:
            errors.append('series_title length exceeds 69 characters')

        # Validity checks
        try:
            released_year = int(row.get('released_year', 0))
            if released_year not in [2014, 2004, 2009]:  # Example valid years
                errors.append('released_year is invalid')
        except ValueError:
            errors.append('released_year is not an integer')

        try:
            imdb_rating = float(row.get('imdb_rating', 0))
            if imdb_rating < 6.6 or imdb_rating > 8:
                errors.append('imdb_rating is out of range')
        except ValueError:
            errors.append('imdb_rating is not a valid float')

        if errors:
            yield {"row": row, "errors": errors}  # Bad data
        else:
            yield row  # Good data


class TransformSchema(beam.DoFn):
    """Transform schema to match target schema."""
    def process(self, row):
        try:
            yield {
                "poster_link": row.get("poster_link"),
                "series_title": row.get("series_title"),
                "released_year": int(row.get("released_year", 0)),
                "certificate": row.get("certificate"),
                "runtime": row.get("runtime"),
                "genre": row.get("genre"),
                "imdb_rating": float(row.get("imdb_rating", 0)),
                "overview": row.get("overview"),
                "meta_score": int(row.get("meta_score", 0)),
                "director": row.get("director"),
                "star1": row.get("star1"),
                "star2": row.get("star2"),
                "star3": row.get("star3"),
                "star4": row.get("star4"),
                "no_of_votes": int(row.get("no_of_votes", 0)),
                "gross": row.get("gross"),
            }
        except (ValueError, KeyError) as e:
            logging.error(f"Schema transformation failed for row: {row}, Error: {e}")


def run():
    # Beam pipeline options
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Use DataflowRunner for GCP
        project='YOUR_PROJECT_ID',
        region='YOUR_PROJECT_REGION',
        temp_location='gs://TEMP_DATA_BUCKET_NAME/temp',
        save_main_session=True,
        streaming=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        raw_data = (
            p
            | "Read from GCS" >> beam.io.ReadFromText(
                'gs://RAW_DATA_BUCKET_NAME/raw/movie_dataset.csv', skip_header_lines=1)
            | "Parse CSV" >> beam.Map(lambda x: dict(zip(
                ['poster_link', 'series_title', 'released_year', 'certificate', 'runtime', 'genre', 'imdb_rating',
                 'overview', 'meta_score', 'director', 'star1', 'star2', 'star3', 'star4', 'no_of_votes', 'gross'],
                x.split(',')
            )))
        )

        # Apply data quality checks
        dq_results = raw_data | "Evaluate Data Quality" >> beam.ParDo(EvaluateDataQuality())

        # Split into good and bad data
        good_data = dq_results | "Filter Good Data" >> beam.Filter(lambda x: "errors" not in x)
        bad_data = dq_results | "Filter Bad Data" >> beam.Filter(lambda x: "errors" in x)

        # Transform schema for good data
        transformed_data = good_data | "Transform Schema" >> beam.ParDo(TransformSchema())

        # Write good data to BigQuery
        transformed_data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table='YOUR_PROJECT_ID:YOUR_BIGQUERY_DATASET.transformed_movie_data',
            schema='SCHEMA_AUTODETECT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Write bad data to GCS
        bad_data | "Write Bad Data to GCS" >> beam.io.WriteToText(
            'gs://RAW_DATA_BUCKET_NAME/bad_data/errors.json',
            file_name_suffix='.json'
        )


if __name__ == "__main__":
    run()
