import asyncio
import logging
import pathlib
from dataclasses import dataclass
from urllib.parse import urlparse

from minio import Minio


@dataclass
class MinioObject:
    name: str
    etag: str
    size: int
    local_path: str | None = None


class MinioClient:
    """Minio client with our app-specific customised functionality."""

    def __init__(
        self,
        url: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
    ) -> None:
        """Initialises Minio client based on given configuration."""
        params = urlparse(url)

        if params.scheme == "s3":
            params.scheme = "https"
        if params.scheme != "http" and params.scheme != "https":
            raise ValueError("Invalid Storage URL – only s3/http(s) schemes supported.")
        if len(params.netloc) == 0:
            raise ValueError("Invalid Storage URL – missing hostname")
        if len(params.path) < 2:
            raise ValueError("Invalid Storage URL – missing bucket name in path")

        bucket_name = params.path[1:].strip("/")
        dir_path = ""

        if "/" in bucket_name:
            split_pos = bucket_name.index("/")
            dir_path, bucket_name = bucket_name[split_pos:]
            bucket_name = bucket_name[:split_pos]

        self._log = logging.getLogger(__name__)
        self._log.info(
            "Calling Minio by URL [%s]",
            url,
            extra={
                "endpoint": params.netloc,
                "secure": params.scheme == "https",
                "bucket": bucket_name,
                "base_dir": dir_path,
            },
        )

        self._bucket_name = bucket_name
        self._dir_path = dir_path
        self._client = Minio(
            endpoint=params.netloc,
            access_key=access_key,
            secret_key=secret_key,
            secure=params.scheme == "https",
            region=region,
        )

    def ping(self) -> bool:
        if self._client.bucket_exists(self._bucket_name):
            try:
                self._client.list_objects(self._bucket_name, self._dir_path)
                return True
            except Exception as e:
                self._log.warning(
                    "Failed to list S3 objects at path [%s] due to: %s",
                    self._dir_path,
                    e,
                )
        return False

    async def list_objects(self, suffix: str) -> list[MinioObject]:
        """List all objects (name and checksum) with given suffix.

        Args:
            suffix: Suffix of listed objects.

        Returns:
            List of objects.
        """
        path_prefix = self._dir_path
        self._log.debug(
            "Listing objects with prefix [%s] and suffix [%s].",
            path_prefix,
            suffix,
        )

        objects = await asyncio.to_thread(
            self._client.list_objects,
            bucket_name=self._bucket_name,
            prefix=path_prefix,
            recursive=True,
        )

        results = []
        for obj in objects:
            cleaned_path = self._rm_dir_prefix(obj.object_name)
            self._log.debug("Found S3-object [%s].", cleaned_path)
            results.append(
                MinioObject(
                    name=cleaned_path,
                    etag=obj.etag,
                    size=obj.size,
                )
            )
        return results

    def download(self, object_path: str, dest_path: pathlib.Path) -> None:
        dest_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        object_path = self._add_dir_prefix(object_path)

        self._log.info("Downloading S3-object: [%s].", object_path)
        self._client.fget_object(self._bucket_name, object_path, str(dest_path))
        self._log.debug("Download completed: [%s].", dest_path)

    def _add_dir_prefix(self, object_name: str) -> str:
        object_name = object_name.lstrip("/")
        if self._dir_path == "":
            return object_name
        return self._dir_path[1:] + "/" + object_name

    def _rm_dir_prefix(self, object_name: str) -> str:
        if self._dir_path == "":
            return "/" + object_name
        return object_name[len(self._dir_path) - 1 :]
