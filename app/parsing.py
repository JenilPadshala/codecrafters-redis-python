from collections import deque
from .custom_data_types import NullArray, ErrorResponse

def parse_request(data: bytes):
    """Parses a simple RESP request and returns the command and its arguments."""

    # check if data is minimum length for a RESP command
    if not data or not data.startswith(b'*'):
        return None
    
    try:
        newline_index = data.index(b'\r\n')
    except ValueError:
        return None
    num_elements = int(data[1:newline_index])

    elements = []
    offset = newline_index + 2  # Move past the initial line
    for _ in range(num_elements):
        if data[offset:offset+1] == b'$': # bulk string
            try:
                next_newline = data.index(b'\r\n', offset)
            except ValueError:
                return None
            str_len = int(data[offset+1:next_newline])
            offset = next_newline + 2  # Move past the length line
            element = data[offset:offset+str_len]
            elements.append(element.decode())
            offset += str_len + 2  # Move past the string and trailing \r\n
        elif data[offset:offset+1] == b'+': # simple string
            try:
                next_newline = data.index(b'\r\n', offset)
            except ValueError:
                return None
            element = data[offset+1:next_newline]
            elements.append(element.decode())
            offset = next_newline + 2  # Move past the string and trailing \r\n
        elif data[offset:offset+1] == b':': # integer
            try:
                next_newline = data.index(b'\r\n', offset)
            except ValueError:
                return None
            element = int(data[offset+1:next_newline])
            elements.append(element)
            offset = next_newline + 2  # Move past the integer and trailing \r\n
        elif data[offset:offset+1] == b'-': # error
            try:
                next_newline = data.index(b'\r\n', offset)
            except ValueError:
                return None
            element = data[offset+1:next_newline]
            elements.append(element.decode())
            offset = next_newline + 2  # Move past the error and trailing \r\n
        else:
            return None  # Unknown type
    return elements


def build_response(response):
    """Builds a RESP response from a Python object."""
    if isinstance(response, str):
        return b"+" + response.encode() + b"\r\n"
    elif isinstance(response, int):
        return b":" + str(response).encode() + b"\r\n"
    elif isinstance(response, bytes):
        return b"$" + str(len(response)).encode() + b"\r\n" + response + b"\r\n"
    elif isinstance(response, NullArray):
        return b"*-1\r\n"
    elif isinstance(response, deque|list):
        resp_parts = [b"*" + str(len(response)).encode() + b"\r\n"]
        for item in response:
            resp_parts.append(build_response(item))
        return b"".join(resp_parts)
    elif isinstance(response, ErrorResponse):
        return b"-ERR" + response.message.encode() + b"\r\n"
    elif response is None:
        return b"$-1\r\n"
    else:
        raise TypeError("Unsupported response type")