#!/usr/bin/env python
# -*- coding: utf-8 -*-

import shutil
from dataclasses import KW_ONLY, Field, dataclass, field, fields
from datetime import date, datetime
from io import BytesIO
from logging import getLogger
from os import PathLike, makedirs
from os.path import abspath
from pathlib import Path
from pkgutil import get_data
from pprint import pformat
from typing import (
    IO,
    Any,
    Callable,
    Collection,
    Final,
    Optional,
    Protocol,
    Type,
    TypeAlias,
    Union,
    get_args,
)
from urllib.error import URLError
from urllib.parse import ParseResult, parse_qs, urlparse
from urllib.request import Request, urlopen
from zipfile import ZipFile

from pandas import DataFrame, Series, read_csv, read_excel

from .utils import filter_fields_by_type

logger = getLogger(__name__)

StablePathType: TypeAlias = str | PathLike[Any]
FilePathType: TypeAlias = StablePathType | IO
SupportedAttrDataTypes: TypeAlias = DataFrame | Series | dict | list | tuple
FileOrURLType: TypeAlias = FilePathType | Request


UK_DATA_PATH: Final[Path] = Path("uk/data")
DOI_URL_PREFIX: Final[str] = "https://doi.org/"

CURL_USER_AGENT: Final[str] = "curl/7.79.1"

EXCEL_BASE_EXTENSION: Final[str] = ".xls"

EXTENSION_PANDAS_READ_MAPPER: Final[dict[str, Callable]] = {
    ".csv": read_csv,
    EXCEL_BASE_EXTENSION: read_excel,
    EXCEL_BASE_EXTENSION + "x": read_excel,
}

VALID_FILE_EXTENSIONS: Final[tuple[str, ...]] = (
    ".zip",
    *tuple(EXTENSION_PANDAS_READ_MAPPER.keys()),
)

EXTENSIONS_EXPECT_SHEET_PARAMETER: Final[tuple[str, ...]] = tuple(
    extension
    for extension in EXTENSION_PANDAS_READ_MAPPER.keys()
    if extension.startswith(EXCEL_BASE_EXTENSION)
)

# VALID_FILE_EXTENSIONS_NO_DOT: Final[tuple[str, ...]] = tuple(ext[1:] for ext in VALID_FILE_EXTENSIONS)


class MultiplePotentialFilesError(Exception):
    pass


class NoPotentialFilesError(Exception):
    pass


class NotValidCitation(Exception):
    pass


class AutoDownloadPermissionError(Exception):
    ...


@dataclass
class MonthDay:
    month: int = 1
    day: int = 1

    def from_year(self, year: int) -> date:
        return date(year, self.month, self.day)


DEFAULT_ANNUAL_MONTH_DAY: Final[MonthDay] = MonthDay()


def extract_file_name_from_url(url: FileOrURLType) -> str:
    """Extract file name from end of a URL and raise warnings for edge cases."""
    if isinstance(url, Request):
        url = url.full_url
    assert isinstance(url, str)
    parsed: ParseResult = urlparse(url)
    if parsed.query:
        logger.debug(f"Querying and parsing potential file paths from url: {url}")
        possible_files: list[str] = []
        for value in parse_qs(parsed.query).values():
            for extension in VALID_FILE_EXTENSIONS:
                if value[0].endswith(extension):
                    possible_files.append(value[0])
        if not possible_files:
            raise NoPotentialFilesError(
                f"No supported file types auto detected from {url}"
            )
        if len(possible_files) == 1:
            logger.info(f"Returning sole file path found: {possible_files[0]}")
            return Path(possible_files[0]).name
        else:
            raise MultiplePotentialFilesError(
                f"{len(possible_files)} possible data files:\n{pformat(possible_files)}"
            )
    else:
        return Path(urlparse(url).path).name


class DataSaveReadCallable(Protocol):

    """A protocol for standardising different ways of managing data sources."""

    def __call__(
        self,
        url_or_path: FileOrURLType,
        local_path: FilePathType | None = None,
        # zip_file_path: Optional[FilePathType] = None,
        **kwargs: Any,
    ) -> None | Request:
        ...

    # @overload
    # def __call__(
    #     self,
    #     url_or_path: FileOrURLType,
    #     local_path: PathLike | None,
    #     zip_file_path: PathLike,
    #     **kwargs: Any,
    # ) -> None:
    #    ...

    # @overload
    # def __call__(
    #     self,
    #     url_or_path: FileOrURLType,
    #     local_path: FilePathType,
    #     reader: Optional[Callable] = None,
    #     extension_to_func_mapper: dict[str, Callable] = EXTENSION_PANDAS_READ_MAPPER,
    #     **kwargs: Any,
    # ) -> Any | None:
    #     ...


def _download_and_save_file(
    url_or_path: FileOrURLType,
    local_path: Optional[FilePathType] = None,
    # zip_file_path: Optional[PathLike] = None,
    **kwargs: Any,
) -> None:
    assert isinstance(url_or_path, str | Request)
    if not local_path:
        local_path = extract_file_name_from_url(url_or_path)
    # assert zip_file_path is None  # Prevent passing to urlopen
    with (
        urlopen(url_or_path, **kwargs) as response,
        open(str(local_path), "wb") as out_file,
    ):
        shutil.copyfileobj(response, out_file)


def _download_unzip_and_save_file(
    url_or_path: FileOrURLType,
    local_path: Optional[PathLike],
    zip_file_path: PathLike,
    **kwargs: Any,
) -> None:
    if not local_path:
        raise ValueError(
            f"'local_path' needed to specify what to extract from a zip resource"
        )
    if isinstance(url_or_path, get_args(FilePathType)):
        raise NotImplementedError(
            f"No implementation yet for unzipping local files like {url_or_path}"
        )
    assert isinstance(url_or_path, Request)
    with ZipFile(BytesIO(urlopen(url_or_path, **kwargs).head())) as zip_files:
        logger.info(f"Extracting '{zip_file_path}' to save to '{local_path}' ...")
        with (
            zip_files.open(str(zip_file_path)) as zip_file,
            open(local_path, "wb") as out_file,
        ):
            shutil.copyfileobj(zip_file, out_file)


def download_and_save_file(
    url_or_path: FileOrURLType,
    local_path: Optional[FilePathType] = None,
    zip_file_path: Optional[PathLike] = None,
    get_and_save_func: DataSaveReadCallable = _download_and_save_file,
    user_agent: Optional[str] = CURL_USER_AGENT,
    headers: dict[str, Any] = {},
    expects_sheet_parameter: tuple[str, ...] = EXTENSIONS_EXPECT_SHEET_PARAMETER,
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
    assert isinstance(url_or_path, str)
    request = Request(url_or_path, headers=headers, **kwargs)
    logger.debug(f"Preparing to read using user-agent {user_agent}")
    if zip_file_path:
        kwargs = dict(kwargs, zip_file_path=zip_file_path)
        if get_and_save_func == _download_and_save_file:
            logger.warning(
                f"zip_file_path: {zip_file_path} provided, "
                f"defaulting to `get_and_save_func`:{get_and_save_func}"
            )
            get_and_save_func = _download_unzip_and_save_file  # type: ignore[assignment]
    try:
        if Path(str(local_path)).suffix in expects_sheet_parameter:
            # `expects_sheet_parameter` means it's likely an xls or xlsx file
            # and a `sheet` needs to be specified, otherwise it defaults to the first
            # available sheet in the spreadsheet
            if "sheet_name" not in kwargs:
                logger.warning(
                    "`sheet_name` parameter not provided, "
                    "defaulting to excel sheet "
                    f"to load: {local_path}"
                )
        get_and_save_func(request, local_path, **kwargs)
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
    version=1995,
)


CopyrightTRIPS = DataLicense(
    name="World Trade Organisation TRIPS Agreement",
    url=("https://www.wto.org/english/tratop_e/trips_e/intel2_e.htm#copyright"),
    version=3,
)


OECDTermsAndConditions = DataLicense(
    name="OECD Terms and Conditions",
    url="https://www.oecd.org/termsandconditions/",
    version=2018,
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
    region: str | None = None
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
    license: Optional[str | DataLicense] = CopyrightTRIPS
    date_time_obtained: Optional[datetime] = None
    auto_download: Optional[bool] = None
    dates: Optional[list[date] | list[int]] = None
    canonical_date: Optional[date | int] = None
    date_published: Optional[date | int] = None
    make_path: bool = True
    file_name_from_url: bool = True
    needs_scaling: bool = False
    dict_key_appreviation: str | None = None
    unit: str | None = None
    cite_as: str | Callable[[Any], str] | None = None
    _save_func: Optional[DataSaveReadCallable] = download_and_save_file  # type: ignore[assignment]
    _save_func_override: bool = False
    _save_kwargs: dict[str, Any] = field(default_factory=dict)
    _package_data: bool = False
    _package_path: Optional[FilePathType] = Path("uk/data")
    _reader_func: DataSaveReadCallable | Callable | None = None
    _reader_kwargs: dict[str, Any] = field(default_factory=dict)
    # _reader_func_override: bool = False
    _post_read_func: Optional[Callable] = None
    _post_read_kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.region and not self.doi and not self.url:
            raise NotValidCitation(
                f"At least `region`, `doi` or `url` must be specified for {self}"
            )
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

    @property
    def has_read_func(self) -> bool:
        return callable(self._reader_func)

    def read(self) -> Optional[Any]:
        """Read file if self._reader_func defined, else None."""
        if not self.has_read_func:
            logger.error(f"No reader set for {self}")
            return None
        else:
            if self.auto_download:
                logger.info(f"Downloading data for {self}")
                self.save_local()
            if self.is_local:
                assert self._reader_func
                assert self.absolute_save_path
                if self._post_read_func:
                    return self._post_read_func(
                        self._reader_func(
                            self.absolute_save_path, self.path, **self._reader_kwargs
                        ),
                        **self._post_read_kwargs,
                    )
                else:
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
        if self._save_func_override:
            self._save_func(**self._save_kwargs)
        else:
            self._save_func(
                self.url, local_path=self.absolute_save_path, **self._save_kwargs
            )
        self.date_time_obtained = datetime.now()

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


#
#
# def read_meta_data(meta_source: MetaData)-> DataFrame:
#     if not meta_source.is_local:
#         meta_source.save_local()
#     return meta_source.read()


def download_and_extract_zip_file(
    url: FileOrURLType,
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
    assert isinstance(url, str)
    zip_request = Request(url, headers={"User-Agent": user_agent})
    with ZipFile(BytesIO(urlopen(zip_request).read())) as zip_files:
        logger.info(f"Extracting '{zip_file_path}' to save to '{local_path}' ...")
        with (
            zip_files.open(str(zip_file_path)) as zip_file,
            open(local_path, "wb") as out_file,
        ):
            shutil.copyfileobj(zip_file, out_file)


def read_package_data(
    file_name: FilePathType, folder: StablePathType = UK_DATA_PATH
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
    folder: StablePathType = UK_DATA_PATH,
) -> Union[FilePathType, BytesIO]:
    if path is default_file:
        logger.info(f"Loading from package data {default_file}.")
        return read_package_data(default_file, folder)
    else:
        return path


def pandas_from_path_or_package(
    url_or_path: FileOrURLType,
    local_path: FilePathType,
    reader: Optional[Callable] = None,
    extension_to_func_mapper: dict[str, Callable] = EXTENSION_PANDAS_READ_MAPPER,
    **kwargs,
) -> Optional[DataFrame]:
    """Import a data file as a DataFrame, managing if package_data used."""
    if (
        isinstance(url_or_path, str)
        or isinstance(url_or_path, PathLike)
        and Path(url_or_path).is_absolute()
    ):
        url_or_path = path_or_package_data(url_or_path, local_path)
    if not reader:
        if isinstance(url_or_path, IO):
            logger.error(f"No reader provided for {url_or_path}")
            return None
        try:
            assert isinstance(url_or_path, str | PathLike)
            reader = reader or extension_to_func_mapper[Path(url_or_path).suffix]
        except KeyError as error:
            logger.error(f"No reader provided or available {error}")
            return None
    return reader(url_or_path, **kwargs)


# def pandas_from_path_or_package_csv(
#     path: FilePathType,
#     default_file: FilePathType,
#     **kwargs,
# ) -> DataFrame:
#     """Import a csv file as a DataFrame, managing if package_data used.
#
#     Todo:
#         * Replace with pandas_from_path_or_package
#     """
#     path = path_or_package_data(path, default_file)
#     return read_csv(path, **kwargs)

MetaFileOrDataFrameType = SupportedAttrDataTypes | FilePathType | MetaData


# def data_frame_path_or_meta_data(
#     source: MetaFileOrDataFrameType,
#     parser: Callable = pandas_from_path_or_package,
# ) -> DataFrame:
#     if isinstance(source, DataFrame):
#         logger.debug(f"DataFrame of size {source.size} passed")
#         return source
#     elif isinstance(source, MetaData):
#         if hasattr(source, 'read') and callable(source.read):
#             logger.debug(f"Calling MetaData read method {source.read}.")
#             return source.read()
#         else:
#             logger.debug(f"MetaData without read method passed. Accessing data from {source.path} via {parser}.")
#             return parser(source.path)
#     else:
#         logger.debug(f"Parsing {source} with parser {parser}.")
#         parser(source)


# def copy_attr_with_prefix(
#     cls: Any,
#     attr_name: str,
#     new_attr_suffix: str,
# ) -> None:
#     setattr(cls, attr_name + new_attr_suffix, getattr(cls, attr_name))


# def attr_path_to_data(
#     cls: 'ModelDataSourcesHandler',
#     data_attr_name: str,
#     path_attr_name: str | None = None,
#     reader_func: Callable = pandas_from_path_or_package,
#     **kwargs: Any,
# ) -> None:
#     if not path_attr_name:
#         path_attr_name = data_attr_name
#     path: Path = Path(getattr(cls, path_attr_name))
#     data: SupportedAttrDataTypes = reader_func(path, **kwargs)
#     setattr(cls, data_attr_name, data)


@dataclass
class ModelDataSourcesHandler:

    """A mixin class for handling MetaFieldOrFrameType attributes."""

    _: KW_ONLY
    _default_data_source_parser: Callable = pandas_from_path_or_package
    _filter_fields_by_type_func: Callable[
        [Any, Type], tuple[Field, ...]
    ] = filter_fields_by_type

    def _filter_fields_by_type(self, field_type: Type | TypeAlias) -> tuple[Field, ...]:
        return self._filter_fields_by_type_func(self, field_type)

    def _get_field_by_name(self, field_name) -> Field:
        for field_attr in fields(self):
            if field_attr.name == field_name:
                return field_attr
        raise ValueError(f"No field {field_name} found in {self}.")

    @property
    def _meta_file_or_dataframe_attrs(self) -> tuple[Field, ...]:
        return self._filter_fields_by_type(field_type=MetaFileOrDataFrameType)

    @property
    def _meta_data_attrs(self) -> tuple[Field, ...]:
        return self._filter_fields_by_type(field_type=MetaData)

    @property
    def _file_or_path_data_attrs(self) -> tuple[Field, ...]:
        return self._filter_fields_by_type(field_type=FilePathType)

    def _set_all_meta_data_fields(
        self,
        parser: Callable | None = None,
        force_default_parser: bool = False,
        **kwargs,
    ) -> None:
        if not parser or force_default_parser:
            parser = self._default_data_source_parser
        for meta_field in self._meta_data_attrs:
            self._set_meta_field(meta_field, parser, force_default_parser, **kwargs)

    def _set_meta_field(
        self,
        meta_field: Field,
        parser: Callable | None = None,
        force_default_parser: bool = False,
        **kwargs,
    ) -> SupportedAttrDataTypes:
        logger.debug(f"Processing {meta_field.name} data file for: {self}")
        if not parser or force_default_parser:
            parser = self._default_data_source_parser
        meta_data: MetaData = getattr(self, meta_field.name)
        if hasattr(meta_data, "url"):
            if not meta_data.auto_download and not meta_data.is_local:
                raise AutoDownloadPermissionError(
                    f"Permission for `auto_download` for field {meta_field} for {self} is false. Consider altering {meta_data} `auto_download` to true."
                )
        setattr(self, f"_{meta_field.name}_meta_data", meta_data)
        assert hasattr(meta_data, "path")
        setattr(self, f"_{meta_field.name}_path", meta_data.path)
        if hasattr(meta_data, "url"):
            setattr(self, f"_{meta_field.name}_url", meta_data.url)
        data: SupportedAttrDataTypes
        if meta_data.has_read_func:
            data = meta_data.read()
        else:
            data = parser(
                path=meta_data.path, default_file=meta_field.default, **kwargs
            )
        setattr(self, meta_field.name, data)
        return data

    def _set_file_or_path_field(
        self,
        file_or_path_field: Field,
        parser: Callable | None,
        force_default_parser: bool = False,
        **kwargs,
    ) -> SupportedAttrDataTypes:
        logger.debug(f"Processing {file_or_path_field.name} data file for: {self}")
        if not parser or force_default_parser:
            parser = self._default_data_source_parser
        file_or_path: FilePathType = getattr(self, file_or_path_field.name)
        setattr(self, f"_{file_or_path_field.name}_path", file_or_path)
        data = parser(
            url_or_path=file_or_path, local_path=file_or_path_field.default, **kwargs
        )
        setattr(self, file_or_path_field.name, data)
        return data

    def _set_all_path_data_fields(
        self,
        parser: Callable | None = None,
        force_default_parser: bool = False,
        **kwargs,
    ) -> None:
        if not parser:
            parser = self._default_data_source_parser
        for file_or_path_field in self._file_or_path_data_attrs:
            _ = self._set_file_or_path_field(
                file_or_path_field, parser, force_default_parser, **kwargs
            )

    def _set_meta_file_or_data_field(
        self,
        attr_field: Field | str,
        parser: Callable | None = None,
        force_default_parser: bool = False,
        **kwargs,
    ) -> SupportedAttrDataTypes:
        if isinstance(attr_field, str):
            attr_field = self._get_field_by_name(attr_field)
        value = getattr(self, attr_field.name)
        if not parser:
            parser = self._default_data_source_parser
        if isinstance(value, get_args(SupportedAttrDataTypes)):
            return value
        elif isinstance(value, MetaData):
            return self._set_meta_field(
                attr_field, parser, force_default_parser, **kwargs
            )
        elif isinstance(value, get_args(FilePathType)):
            return self._set_file_or_path_field(
                attr_field, parser, force_default_parser, **kwargs
            )
        else:
            raise TypeError(
                f"Type of field {type(value)} not supported " f"for {attr_field.name}"
            )

    def _set_all_meta_file_or_data_fields(
        self,
        parser: Callable | None = None,
        force_default_parser: bool = False,
        **kwargs,
    ) -> None:
        if not parser:
            parser = self._default_data_source_parser
        for meta_file_or_dataframe_field in self._meta_file_or_dataframe_attrs:
            _: SupportedAttrDataTypes = self._set_meta_file_or_data_field(
                meta_file_or_dataframe_field, parser, force_default_parser, **kwargs
            )

    # def _path_to_data(self,
    #                   data_attr_name: str,
    #                   path_attr_name: str | None = None,
    #                   reader_func: Callable | None = None,
    #                   **kwargs: Any,
    #                   ) -> None:
    #     if not reader_func:
    #         reader_func = self._default_data_parser
    #     attr_path_to_data(self, data_attr_name, path_attr_name, reader_func, **kwargs)

    # if not path_attr_name:
    #     path_attr_name = data_attr_name
    # setattr(self, )
