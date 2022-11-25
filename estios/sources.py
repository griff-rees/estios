#!/usr/bin/env python
# -*- coding: utf-8 -*-

import shutil
from dataclasses import dataclass, field
from datetime import date, datetime
from io import BytesIO
from logging import getLogger
from os import PathLike, makedirs
from os.path import abspath
from pathlib import Path
from pkgutil import get_data
from pprint import pformat
from typing import IO, Any, Callable, Collection, Final, Optional, Protocol, Union
from urllib.error import URLError
from urllib.parse import ParseResult, parse_qs, urlparse
from urllib.request import Request, urlopen
from zipfile import ZipFile

from pandas import DataFrame, read_csv, read_excel

logger = getLogger(__name__)

FilePathType = Union[str, IO, PathLike]
FolderPathType = Union[str, PathLike]

UK_DATA_PATH: Final[Path] = Path("uk/data")
DOI_URL_PREFIX: Final[str] = "https://doi.org/"

CURL_USER_AGENT: Final[str] = "curl/7.79.1"


EXTENSION_PANDAS_READ_MAPPER: Final[dict[str, Callable]] = {
    ".csv": read_csv,
    ".xls": read_excel,
    ".xlsx": read_excel,
}

VALID_FILE_EXTENSIONS: Final[tuple[str, ...]] = (
    ".zip",
    *tuple(EXTENSION_PANDAS_READ_MAPPER.keys()),
)


class MultplePotentialFilesError(Exception):
    pass


@dataclass
class MonthDay:
    month: int = 1
    day: int = 1

    def from_year(self, year: int) -> date:
        return date(year, self.month, self.day)


DEFAULT_ANNUAL_MONTH_DAY: Final[MonthDay] = MonthDay()


def extract_file_name_from_url(url: str | Request) -> str:
    """Extract file name from end of a URL and raise warnings for edge cases."""
    if isinstance(url, Request):
        url = url.full_url
    parsed: ParseResult = urlparse(url)
    if parsed.query:
        logger.debug("Query URL, parsing potential file paths.")
        possible_files: list[str] = []
        for value in parse_qs(parsed.query).values():
            for extension in VALID_FILE_EXTENSIONS:
                if value[0].endswith(extension):
                    possible_files.append(value[0])
        if len(possible_files) == 1:
            logger.info(f"Returning sole file path found: {possible_files[0]}")
            return Path(possible_files[0]).name
        else:
            raise MultplePotentialFilesError(
                f"{len(possible_files)} possible data files:\n{pformat(possible_files)}"
            )
    else:
        return Path(urlparse(url).path).name


class DataSaverCallable(Protocol):

    """A protocol for standardising different ways of managing data sources."""

    def __call__(
        self,
        url_or_path: str | Request,
        local_path: Optional[FilePathType] = None,
        # zip_file_path: Optional[FilePathType] = None,
        **kwargs: Any,
    ) -> None:
        ...


def _download_and_save_file(
    url_or_path: str | Request,
    local_path: Optional[FilePathType] = None,
    # zip_file_path: Optional[PathLike] = None,
    **kwargs: Any,
) -> None:
    if not local_path:
        local_path = extract_file_name_from_url(url_or_path)
    # assert zip_file_path is None  # Prevent passing to urlopen
    with (
        urlopen(url_or_path, **kwargs) as response,
        open(str(local_path), "wb") as out_file,
    ):
        return shutil.copyfileobj(response, out_file)


def _download_unzip_and_save_file(
    url_or_path: str | Request,
    local_path: Optional[PathLike],
    zip_file_path: PathLike,
    **kwargs: Any,
) -> None:
    if not local_path:
        raise ValueError(
            f"'local_path' needed to specify what to extract from a zip resource"
        )
    if isinstance(url_or_path, str):
        raise NotImplementedError(
            f"No implementation yet for unzipping local files like {url_or_path}"
        )
    with ZipFile(BytesIO(urlopen(url_or_path, **kwargs).head())) as zip_files:
        logger.info(f"Extracting '{zip_file_path}' to save to '{local_path}' ...")
        with (
            zip_files.open(str(zip_file_path)) as zip_file,
            open(local_path, "wb") as out_file,
        ):
            return shutil.copyfileobj(zip_file, out_file)


def download_and_save_file(
    url_or_path: str,
    local_path: Optional[FilePathType] = None,
    zip_file_path: Optional[PathLike] = None,
    get_and_save_func: DataSaverCallable = _download_and_save_file,
    user_agent: Optional[str] = CURL_USER_AGENT,
    headers: dict[str, Any] = {},
    **kwargs: Any,
) -> None:
    """Download the file from `url_or_path` and save it to `local_path`"""
    if user_agent:
        logger.debug(f"Setting `User-Agent` to {user_agent} in headers`")
        headers["User-Agent"] = user_agent
    if not local_path and zip_file_path:
        logger.info(
            f"'local_path' not specified, setting to '{Path(zip_file_path).name}'"
        )
        local_path = zip_file_path
    elif not local_path:
        logger.debug(
            f"'local_path' not specified, using {local_path} from {url_or_path}"
        )
        local_path = extract_file_name_from_url(url_or_path)
    logger.info(f"Downloading {url_or_path} to save to {local_path} ...")
    request = Request(url_or_path, headers=headers, **kwargs)
    logger.debug(f"Preparing to read using user-agent {user_agent}")
    if zip_file_path:
        kwargs = dict(kwargs, zip_file_path=zip_file_path)
        if get_and_save_func == _download_and_save_file:
            logger.warning(
                f"zip_file_path: {zip_file_path} provided,"
                f"defaulting to get_and_save_func: "
                f"_download_unzip_and_save_file"
            )
            get_and_save_func = _download_unzip_and_save_file  # type: ignore[assignment]

    try:
        return get_and_save_func(request, local_path, **kwargs)
    except URLError:
        logger.error(
            f"URLError: likely no internet connection, fail downloading {url_or_path}"
        )


@dataclass
class DataLicense:

    """Class for standardising data license references."""

    name: str
    url: str
    version: Optional[int | str]

    def __str__(self) -> str:
        if self.version:
            return f"{self.name} {self.version}"
        else:
            return self.name


OpenGovernmentLicense = DataLicense(
    name="Open Government License",
    url=("https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"),
    version=3,
)


@dataclass
class MetaData:

    """Manage info on source material.

    Todo:
        * Check uk.regions authors example for Collections type case.
        * replace year with years or dates for range of coverage
        * Way of enforcing region naming conventions
        * Testing needed for managing files without fallback package paths.
    """

    name: str
    year: int  #
    region: str
    other_regions: Optional[tuple[str, ...]] = None
    description: Optional[str] = None
    date_created: Optional[date] = None
    authors: Optional[
        Union[
            str,
            tuple[str, ...],
            dict[str, str],
            dict[str, dict[str, str]],
            dict[str, Collection[str]],
        ]
    ] = None
    url: Optional[str] = None
    info_url: Optional[str] = None
    doi: Optional[str] = None
    path: Optional[FilePathType] = None
    license: Optional[str | DataLicense] = None
    date_time_obtained: Optional[datetime] = None
    auto_download: Optional[bool] = None
    dates: Optional[list[date] | list[int]] = None
    canonical_date: Optional[date] = None
    date_published: Optional[date] = None
    make_path: bool = True
    file_name_from_url: bool = True
    needs_scaling: bool = False
    dict_key_appreviation: str | None = None
    _save_func: Optional[DataSaverCallable] = download_and_save_file  # type: ignore[assignment]
    _save_kwargs: dict[str, Any] = field(default_factory=dict)
    _package_data: bool = False
    _package_path: Optional[FilePathType] = Path("uk/data")
    _reader_func: Optional[Callable] = None
    _reader_kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.url and self.doi:
            self.url = DOI_URL_PREFIX + self.doi
        if (
            self.url
            and self.file_name_from_url
            and not isinstance(self.path, IO)
            and "zip_file_path" not in self._save_kwargs
        ):
            url_file_name: str = extract_file_name_from_url(self.url)
            if self.path and Path(self.path).name:
                logger.warning(f"Replacing {Path(self.path).name} with {url_file_name}")
            self.path = url_file_name
            # if self.path == 'file':
            #     breakpoint()
        if self._package_data:
            self.path = str(self._package_path) / Path(str(self.path))
        if self.auto_download:
            if not self.is_local:
                logger.warning(f"Downloading {self}")
                self.save_local()

    def __repr__(self) -> str:
        """A simplified way of demonstrating the core elements of the class.

        See:
            * https://github.com/vcs-python/libvcs/blob/v0.13.0a3/libvcs/utils/dataclasses.py#L5
        """
        return f"{self.__class__.__name__}(name={self.name}, region={self.region}, year={self.year})"

    def __str__(self) -> str:
        return f"Source: {self.name} for {self.region} {self.year}"

    def set_folder(self, folder_path: PathLike) -> None:
        """Change path to passed folder_path while keeping self.path stem."""
        self.path = Path(str(folder_path)) / Path(str(self.path)).name

    def read(self) -> Optional[Any]:
        """Read file if self._reader_func defined, else None."""
        if not self._reader_func:
            logger.error(f"No reader set for {self}")
            return None
        else:
            if self.is_local:
                return self._reader_func(
                    self.absolute_save_path, self.path, **self._reader_kwargs
                )
                # if self._package_data:
                #     return self._reader_func(self.absolute_save_path, self.path, **self._reader_kwargs)
                # else:
                #     logger.warning("Testing needed for managing files without fallback package paths.")
                #     return self._reader_func(self.absolute_save_path, **self._reader_kwargs)

            else:
                logger.error(
                    f"File for {self} not saved locally. Try running the .save_local() method first"
                )
                return None

    @property
    def absolute_save_path(self) -> Optional[PathLike]:
        """Calculate absolute save path based on path of package install.

        Todo:
            * May need to refactor/rename set_folder method.
        """
        if not self.path:
            logger.warning(f"{self} has no 'path' attribute set")
            return None
        elif isinstance(self.path, IO):
            logger.warning(f"{self} cannot extract an 'absolute_save_path' from a IO")
            return None
        if not self.make_path:
            logger.warning(f"{self} 'make_path' set to False.")
            return None
        else:
            if Path(self.path).is_absolute():
                return Path(self.path)
            else:
                return abspath(__package__) / Path(self.path)
            # absolute_save_path: self.path.dirname()
            # if self._package_data:
            #     absolute_save_path = path(__file__)
            #     installed_path: PathLike = path(__file__)
            # assert False
            # makedirs(self.path.dirname(), exist_ok=True)

    def save_local(self, force_overwrite: bool = False) -> None:
        """Get file from self.url and save locally.

        Todo:
            * May need to refactor means of creating local folder below vs _save_func.
        """
        if not self.path:
            logger.error(f"Path must be set to save {self}")
            return
        if not self._save_func:
            logger.error(f"'self._save_func' must be set to run 'self.save_local'")
            return
        if not self.absolute_save_path:
            logger.warning(f"Cannot save locally if 'absolute_save_path' is not set")
            return
        if self.make_path:
            # absolute_save_path: self.path.dirname()
            # if self._package_data:
            #     absolute_save_path = path(__file__)
            #     installed_path: PathLike = path(__file__)
            # assert False
            makedirs(Path(self.absolute_save_path).parent, exist_ok=True)
        if self.is_local:
            if not force_overwrite:
                logger.warning(
                    f"{self.path} already exists. To force set 'force_overwrite' to True"
                )
                return
        try:
            assert self.url
        except AssertionError:
            raise AssertionError(f"{self.url} required to to download and save {self}")
        logger.info(f"Saving {self.url} to {self.path} with {self._save_func}")
        self._save_func(
            self.url, local_path=self.absolute_save_path, **self._save_kwargs
        )
        self.date_time_obtained == datetime.now()

    @property
    def is_local(self) -> bool:
        """Return True if local copy exists, False otherwise."""
        return Path(str(self.absolute_save_path)).is_file()

    def delete_local(self) -> None:
        """Delete local copy of file."""
        if self.is_local:
            logger.info(f"Deleting {self.path} from {self}")
            Path(str(self.absolute_save_path)).unlink()
        else:
            logger.warning(f"Cannot delete {self.path} which does not exist.")


def download_and_extract_zip_file(
    url: str,
    local_path: Optional[PathLike] = None,
    zip_file_path: Optional[PathLike] = None,
    user_agent: str = CURL_USER_AGENT,
) -> None:
    """Download and extract a zip file and return stream."""
    if not zip_file_path:
        raise ValueError(f"'zip_file_path' is necessary for extration")
    logger.info(f"Downloading {url} ...")
    if not local_path:
        logger.info(
            f"'local_path' not specified, setting to '{Path(zip_file_path).name}'"
        )
        local_path = zip_file_path
    logger.debug(f"Preparing to read using user-agent {user_agent}")
    zip_request = Request(url, headers={"User-Agent": user_agent})
    with ZipFile(BytesIO(urlopen(zip_request).read())) as zip_files:
        logger.info(f"Extracting '{zip_file_path}' to save to '{local_path}' ...")
        with (
            zip_files.open(str(zip_file_path)) as zip_file,
            open(local_path, "wb") as out_file,
        ):
            shutil.copyfileobj(zip_file, out_file)


def read_package_data(
    file_name: FilePathType, folder: FolderPathType = UK_DATA_PATH
) -> BytesIO:
    if isinstance(file_name, IO):
        raise NotImplementedError(f"Currently no means of reading {file_name} types.")
    else:
        raw_data = get_data(__package__, str(Path(folder) / file_name))
        if raw_data is None:
            raise NotImplementedError(f"Processing {file_name} returned None.")
        else:
            return BytesIO(raw_data)


def path_or_package_data(
    path: FilePathType,
    default_file: FilePathType,
    folder: FolderPathType = UK_DATA_PATH,
) -> Union[FilePathType, BytesIO]:
    if path is default_file:
        logger.info(f"Loading from package data {default_file}.")
        return read_package_data(default_file, folder)
    else:
        return path


def pandas_from_path_or_package(
    path: FilePathType,
    default_file: FilePathType,
    reader: Optional[Callable] = None,
    extension_to_func_mapper: dict[str, Callable] = EXTENSION_PANDAS_READ_MAPPER,
    **kwargs,
) -> Optional[DataFrame]:
    """Import a data file as a DataFrame, managing if package_data used."""
    if isinstance(path, str) or isinstance(path, PathLike) and Path(path).is_absolute():
        path = path_or_package_data(path, default_file)
    if not reader:
        if isinstance(path, IO):
            logger.error(f"No reader provided for {path}")
            return None
        try:
            reader = reader or extension_to_func_mapper[Path(path).suffix]
        except KeyError as error:
            logger.error(f"No reader provided or available {error}")
            return None
    return reader(path, **kwargs)


def pandas_from_path_or_package_csv(
    path: FilePathType,
    default_file: FilePathType,
    **kwargs,
) -> DataFrame:
    """Import a csv file as a DataFrame, managing if package_data used.

    Todo:
        * Replace with pandas_from_path_or_package
    """
    path = path_or_package_data(path, default_file)
    return read_csv(path, **kwargs)
