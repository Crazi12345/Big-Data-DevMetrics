import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader


def validate_avro_file(avro_file_path, schema_file_path):
    """
    Validates an Avro file against an Avro schema loaded from a file.

    Args:
        avro_file_path (str): The path to the Avro file.
        schema_file_path (str): The path to the file containing the Avro schema.

    Returns:
        bool: True if the file is valid, False otherwise.
    """

    try:
        # Read the schema from the file
        with open(schema_file_path, 'r') as f:
            schema_str = f.read()

        # Parse the schema
        schema = avro.schema.parse(schema_str)

        # Open the Avro data file
        with open(avro_file_path, 'rb') as f:
            # Create a DataFileReader
            reader = DataFileReader(f, DatumReader(schema))

            # Iterate over the records (this will raise an error if any record is invalid)
            for _ in reader:
                pass

            # Close the reader
            reader.close()

        # If no errors were raised, the file is valid
        return True

    except Exception as e:
        print(f"Validation error: {e}")
        return False


# Example usage:
avro_file = 'pp.avro'
schema_file = 'postSchema.avsc'

if validate_avro_file(avro_file, schema_file):
    print(f"File '{avro_file}' is valid according to the schema in '{
          schema_file}'.")
else:
    print(f"File '{avro_file}' is NOT valid according to the schema in '{
          schema_file}'.")
