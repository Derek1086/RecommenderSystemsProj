import os
import logging
import json


# [[INTERNAL]]
# A chunk generating function to split a large json file into chunks.
def generate_chunks(gc_path, gc_size=1024 * 1024):
    file_end = os.path.getsize(gc_path)
    with open(gc_path, 'rb') as fl:
        chunk_end = fl.tell()
        while True:
            chunk_start = chunk_end
            fl.seek(gc_size, 1)
            fl.readline()
            chunk_end = fl.tell()
            yield chunk_start, chunk_end
            if chunk_end > file_end:
                break


# [[INTERNAL]]
# The thread worker function.
def process_chunk(args):
    pc_path, pc_start, pc_end, pc_enc, logger = args
    temp = []
    try:
        with open(pc_path, 'r', encoding=pc_enc) as fl:
            fl.seek(pc_start)  # Go to the beginning of chunk
            while fl.tell() < pc_end:  # Process all lines in the chunk
                line = fl.readline()
                if line:
                    try:
                        temp.append(json.loads(line))
                    except json.JSONDecodeError:
                        logger.error(f'Error occurred processing line: {line}', exc_info=True)
                        continue  # Skip lines that can't be decoded
                else:
                    break  # Reached the end of file
    except Exception as e:
        logger.error(f'The chunk ({pc_start}-{pc_end}) could not be processed.', exc_info=True)
    return temp
