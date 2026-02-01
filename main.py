# Trash code

import gzip
from multiprocessing import Pool, cpu_count
from collections import Counter

CHUNK_SIZE = 20_000

def process_chunk(lines):
    import json
    from collections import Counter

    c = Counter()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            val = obj.get("status")
            if val is not None:
                c[val] += 1
        except json.JSONDecodeError:
            pass
    return c

def chunked_reader(f, size):
    chunk = []
    for line in f:
        chunk.append(line)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

if __name__ == "__main__":
    final = Counter()
    workers = cpu_count()

    with gzip.open("part-00001.gz", "rt") as f, Pool(workers) as p:
        for result in p.imap_unordered(process_chunk, chunked_reader(f, CHUNK_SIZE)):
            final.update(result)

    print(final)

