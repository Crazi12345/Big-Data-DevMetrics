import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

# Define the schema
schema = avro.schema.parse(open("user.avsc", "rb").read())

# Create a writer
writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)

# Create some users
users = [
    {"name": "Alyssa", "favorite_number": 256},
    {"name": "Ben", "favorite_number": 7, "favorite_color": "red"}
]

# Write the users to the file
for user in users:
    writer.append(user)

# Close the writer
writer.close()
