import base64
from typing import Optional

from sync.clients.databricks import DatabricksClient


def format_dbfs_filepath(base_path: str):
    base_path = base_path.strip("/")
    return f"dbfs:/{base_path}" if "dbfs:/" not in base_path else base_path


def read_dbfs_file(
    filepath: str,
    dbx_client: DatabricksClient,
    filesize: Optional[int] = None,
) -> bytes:
    """Given a DBFS filepath, returns that file's content in its entirety"""
    offset = 0
    filepath = format_dbfs_filepath(filepath)

    if filesize is not None:
        bytes_read = 0
        # DBFS tells us exactly how many bytes to expect for each file, so if that size is known,
        #  we can pre-allocate an array to write the file chunks in to
        file_content = bytearray(filesize)
        while bytes_read < filesize:
            chunk = dbx_client.read_dbfs_file_chunk(filepath, offset=bytes_read)
            new_bytes_read = bytes_read + chunk["bytes_read"]
            file_content[bytes_read:new_bytes_read] = base64.b64decode(chunk["data"])
            bytes_read = new_bytes_read
    else:
        file_content = ""
        chunk = dbx_client.read_dbfs_file_chunk(filepath, offset)
        bytes_read = chunk["bytes_read"]
        while bytes_read > 0:
            file_content += chunk["data"]
            offset += bytes_read

            chunk = dbx_client.read_dbfs_file_chunk(filepath, offset)
            bytes_read = chunk["bytes_read"]

        file_content = base64.b64decode(file_content)

    return file_content


_WRITE_CHUNK_SIZE = 1024 * 1024


def write_dbfs_file(filepath: str, body: bytes, dbx_client: DatabricksClient) -> None:
    """Given a DBFS filepath and some bytes, writes the data in its entirety to DBFS"""
    filepath = format_dbfs_filepath(filepath)
    encoded = base64.b64encode(body).decode("utf-8")
    fd = dbx_client.open_dbfs_file_stream(filepath, True)["handle"]

    bytes_written = 0
    total_bytes = len(encoded)
    while bytes_written < total_bytes:
        chunk_end = min(total_bytes, bytes_written + _WRITE_CHUNK_SIZE)
        content = encoded[bytes_written:chunk_end]
        dbx_client.add_block_to_dbfs_file_stream(fd, content)
        bytes_written += len(content)

    dbx_client.close_dbfs_file_stream(fd)
