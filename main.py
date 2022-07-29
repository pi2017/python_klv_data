#!/usr/bin/env python3
import klvdata
import subprocess as sp
import shlex
import threading
import numpy as np
import cv2
from io import BytesIO


# Video reader thread.
def video_reader(pipe):
    cols, rows = 1280, 720  # Assume we know frame size is 1280x720

    counter = 0
    while True:
        raw_image = pipe.read(cols * rows * 3)  # Read raw video frame

        # Break the loop when length is too small
        if len(raw_image) < cols * rows * 3:
            break

        if (counter % 60) == 0:
            # Show video frame evey 60 frames
            image = np.frombuffer(raw_image, np.uint8).reshape([rows, cols, 3])
            cv2.imshow('Video KLV', image)  # Show video image for testing
            cv2.waitKey(1)
        counter += 1


# https://github.com/paretech/klvdata/tree/master/klvdata
def bytes_to_int(value, signed=False):
    """Return integer given bytes."""
    return int.from_bytes(bytes(value), byteorder='big', signed=signed)


# Data reader thread (read KLV data).
def data_reader(pipe):
    key_length = 16  # Assume key length is 16 bytes.

    f = open('data.bin', 'wb')  # For testing - store the KLV data to data.bin (binary file)

    while True:
        # https://en.wikipedia.org/wiki/KLV
        # The first few bytes are the Key, much like a key in a standard hash table data structure.
        # Keys can be 1, 2, 4, or 16 bytes in length.
        # Presumably in a separate specification document you would agree on a key length for a given application.
        key = pipe.read(key_length)  # Read the key

        if len(key) < key_length:
            break  # Break the loop when length is too small
        f.write(key)  # Write data to binary file for testing

        # https://github.com/paretech/klvdata/tree/master/klvdata
        # Length field
        len_byte = pipe.read(1)

        if len(len_byte) < 1:
            break  # Break the loop when length is too small
        f.write(len_byte)  # Write data to binary file for testing

        byte_length = bytes_to_int(len_byte)

        # https://github.com/paretech/klvdata/tree/master/klvdata
        if byte_length < 128:
            # BER Short Form
            length = byte_length
            ber_len_bytes = b''
        else:
            # BER Long Form
            ber_len = byte_length - 128
            ber_len_bytes = pipe.read(ber_len)

            if len(ber_len_bytes) < ber_len:
                break  # Break the loop when length is too small
            f.write(ber_len_bytes)  # Write ber_len_bytes to binary file for testing

            length = bytes_to_int(ber_len_bytes)

        # Read the value (length bytes)
        value = pipe.read(length)
        if len(value) < length:
            break  # Break the loop when length is too small
        f.write(value)  # Write data to binary file for testing

        klv_data = key + len_byte + ber_len_bytes + value  # Concatenate key length and data
        klv_data_as_bytes_io = BytesIO(klv_data)  # Wrap klv_data with BytesIO (before parsing)

        # Parse the KLV data
        for packet in klvdata.StreamParser(klv_data_as_bytes_io):
            metadata = packet.MetadataList()
            print(metadata)
            print()  # New line


# Execute FFmpeg as sub-process
# Map the video to stderr and map the data to stdout
process = sp.Popen(
    shlex.split('d:/ffmpeg/bin/ffmpeg -hide_banner -loglevel quiet '  # Set loglevel to quiet for disabling the prints ot stderr
                '-i "source/Day Flight.mpg" '  # Input video "Day Flight.mpg"
                '-map 0:v -c:v rawvideo -pix_fmt bgr24 -f:v rawvideo pipe:2 '  # rawvideo format is mapped to stderr pipe (raw video codec with bgr24 pixel format)
                '-map 0:d -c copy -copy_unknown -f:d data pipe:1 '  # Copy the data without ddecoding.
                '-report'),  # Create a log file (because we can't the statuses that are usually printed to stderr).
    stdout=sp.PIPE, stderr=sp.PIPE)

# Start video reader thread (pass stderr pipe as argument).
video_thread = threading.Thread(target=video_reader, args=(process.stderr,))
video_thread.start()

# Start data reader thread (pass stdout pipe as argument).
data_thread = threading.Thread(target=data_reader, args=(process.stdout,))
data_thread.start()

# Wait for threads (and process) to finish.
video_thread.join()
data_thread.join()
process.wait()