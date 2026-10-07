"""Lossless gzip storage for HTML caches; other file types retain Path behavior.

Callers keep logical .html/.htm paths. Both legacy plain files and .gz siblings
are readable; new HTML writes are atomic gzip files. Temporary .html.tmp paths
are supported so existing validate-then-publish workflows still work.
"""
import gzip
import io
import os
from pathlib import Path
import tempfile


def is_html(path):
    name = Path(path).name.lower()
    if name.endswith(".gz"):
        name = name[:-3]
    if name.endswith(".tmp"):
        name = name[:-4]
    return name.endswith((".html", ".htm"))


def compressed_path(path):
    path = Path(path)
    return path if path.suffix == ".gz" else Path(str(path) + ".gz")


def resolve(path):
    path = Path(path)
    packed = compressed_path(path)
    if is_html(path) and packed.is_file():
        return packed
    return path


def exists(path):
    return resolve(path).exists()


def is_file(path):
    return resolve(path).is_file()


def stat(path, **kwargs):
    return resolve(path).stat(**kwargs)


def read_bytes(path):
    path = resolve(path)
    if is_html(path) and path.suffix == ".gz":
        with gzip.open(path, "rb") as stream:
            return stream.read()
    return path.read_bytes()


def read_text(path, encoding=None, errors=None):
    path = resolve(path)
    if is_html(path) and path.suffix == ".gz":
        with gzip.open(path, "rt", encoding=encoding or "utf-8", errors=errors) as stream:
            return stream.read()
    return path.read_text(encoding=encoding, errors=errors)


def _publish_gzip(path, data):
    """Publish only a complete gzip; leave the previous cache on failure."""
    target = compressed_path(path)
    fd, name = tempfile.mkstemp(prefix=target.name + ".", suffix=".tmp", dir=target.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            with gzip.GzipFile(filename="", mode="wb", fileobj=stream,
                               compresslevel=6, mtime=0) as packed:
                packed.write(data)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def write_bytes(path, data):
    path = Path(path)
    if not is_html(path):
        return path.write_bytes(data)
    _publish_gzip(path, data)
    if path.suffix != ".gz":
        path.unlink(missing_ok=True)
    return len(data)


def write_text(path, data, encoding=None, errors=None, newline=None):
    path = Path(path)
    if not is_html(path):
        return path.write_text(data, encoding=encoding, errors=errors, newline=newline)
    # Match text-file newline and encoding behavior before compression.
    buffer = io.BytesIO()
    with io.TextIOWrapper(buffer, encoding=encoding or "utf-8",
                          errors=errors, newline=newline) as stream:
        count = stream.write(data)
        stream.flush()
        write_bytes(path, buffer.getvalue())
    return count


def replace(source, target):
    source, target = Path(source), Path(target)
    actual = resolve(source)
    if is_html(source) and actual.suffix == ".gz":
        actual.replace(compressed_path(target))
        if target.suffix != ".gz":
            target.unlink(missing_ok=True)
        if source != actual:
            source.unlink(missing_ok=True)
        return target
    return source.replace(target)


def unlink(path, missing_ok=False):
    path = Path(path)
    if not is_html(path):
        return path.unlink(missing_ok=missing_ok)
    present = False
    for candidate in {path, compressed_path(path)}:
        if candidate.exists():
            candidate.unlink()
            present = True
    if not present and not missing_ok:
        raise FileNotFoundError(path)


def html_files(directory):
    """Find immediate HTML children once, whether plain, gzip, or both."""
    paths = set()
    for path in Path(directory).iterdir():
        if path.is_file() and is_html(path) and not path.name.endswith((".tmp", ".tmp.gz")):
            paths.add(Path(str(path)[:-3]) if path.suffix == ".gz" else path)
    return sorted(paths)


def compress_existing(path):
    """Verify decompression byte-for-byte before removing a legacy HTML file."""
    path = Path(path)
    if not is_html(path) or path.suffix == ".gz":
        raise ValueError(f"Expected an uncompressed HTML path: {path}")
    before = path.stat()
    data = path.read_bytes()
    target = compressed_path(path)
    if target.exists():
        with gzip.open(target, "rb") as stream:
            if stream.read() != data:
                raise ValueError(f"Conflicting plain and gzip cache: {path}")
    else:
        _publish_gzip(path, data)
    with gzip.open(target, "rb") as stream:
        if stream.read() != data:
            raise ValueError(f"Gzip verification failed: {path}")
    # Do not remove a cache that another writer changed during compression.
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ino) != (
            after.st_size, after.st_mtime_ns, after.st_ino):
        raise RuntimeError(f"Cache changed during compression: {path}")
    os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
    path.unlink()
    return before.st_blocks * 512, target.stat().st_blocks * 512


def main():
    """Compress existing HTML in a data tree, verifying every file first."""
    import argparse
    from concurrent.futures import ThreadPoolExecutor
    import json
    import time

    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--root", type=Path, default=Path(".data"))
    parser.add_argument("--workers", type=int, choices=range(1, 9), default=4)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if not args.root.is_dir():
        parser.error(f"Not a directory: {args.root}")
    paths = sorted(p for p in args.root.rglob("*")
                   if p.suffix.lower() in (".html", ".htm") and p.is_file()
                   and not p.is_symlink())
    before = after = 0
    started = time.monotonic()
    print(f"Compressing {len(paths):,} HTML files with byte-for-byte verification.", flush=True)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for count, (old, new) in enumerate(pool.map(compress_existing, paths), 1):
            before += old
            after += new
            if count % 5000 == 0 or count == len(paths):
                print(f"{count:,}/{len(paths):,}: saved {(before-after)/1024**3:.2f} GiB", flush=True)
    report = {"files_verified": len(paths), "disk_bytes_before": before,
              "disk_bytes_after": after, "disk_bytes_saved": before-after,
              "elapsed_seconds": round(time.monotonic()-started, 2)}
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
