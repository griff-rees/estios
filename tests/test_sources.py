from copy import deepcopy
from dataclasses import dataclass, field
from logging import DEBUG
from os import PathLike
from pathlib import Path
from typing import Final, Iterable

import pytest
from numpy import ndarray, savetxt
from numpy.random import randint
from pandas import DataFrame

from estios.sources import (
    AutoDownloadPermissionError,
    FilePathType,
    MetaData,
    MetaFileOrDataFrameType,
    ModelDataSourcesHandler,
    download_and_save_file,
    extract_file_name_from_url,
    pandas_from_path_or_package,
)
from estios.uk.ons_population_projections import (
    ONS_ENGLAND_POPULATION_PROJECTIONS_FILE_NAME,
    ONS_ENGLAND_POPULATIONS_PROJECTION_2018_ZIP_URL,
)
from estios.uk.ons_uk_population_projections import ONS_UK_POPULATION_META_DATA
from estios.utils import field_names, filter_fields_by_type


@pytest.mark.remote_data
class TestDownloadingDataFiles:

    """Test downloading and storing datafiles, skipping if no internet connection."""

    jpg_url: str = "https://commons.wikimedia.org/wiki/File:Wassily_Leontief_1973.jpg"
    input_output_example_zip: str = ONS_ENGLAND_POPULATIONS_PROJECTION_2018_ZIP_URL
    zip_file_path: PathLike = ONS_ENGLAND_POPULATION_PROJECTIONS_FILE_NAME

    def test_extract_file_name_from_url(self) -> None:
        """Test a simple extractiong of a filename from a URL."""
        correct_file_name: str = self.jpg_url.split("/")[-1]
        assert extract_file_name_from_url(self.jpg_url) == correct_file_name

    @pytest.mark.xfail
    def test_download_extract_zip_custom_local_name(self, tmp_path) -> None:
        """Test downloading and extracting remote zip file to custom local path.

        Note:
            Original example:
                https://www.oecd.org/industry/ind/
                input-outputtableslatesteditionaccesstodata.htm

            raised http.client.IncompleteRead errors
        """
        # input_output_example_zip: str = "https://www.oecd.org/sti/ind/42163955.zip"
        # local_path: PathLike = "zaf2005.xls"
        local_file_name: str = "test_extract.csv"
        download_and_save_file(
            self.input_output_example_zip,
            tmp_path / local_file_name,
            zip_file_path=self.zip_file_path,
        )
        with open(tmp_path / local_file_name) as test_saved_file:
            assert test_saved_file.name.endswith(local_file_name)

    @pytest.mark.xfail
    def test_download_extract_zip(self, tmp_path, caplog, monkeypatch) -> None:
        """Test downloading and extracting remote zip file to same name.

        Note:
            Error probably from ONS /file?uri= error 2018snppopulation.zip
        """
        monkeypatch.chdir(tmp_path)  # Enforce location to fit tmp_path
        caplog.set_level(DEBUG)
        download_and_save_file(
            self.input_output_example_zip,
            zip_file_path=self.zip_file_path,
        )
        with open(self.zip_file_path) as test_saved_file:
            assert test_saved_file.name == str(self.zip_file_path)
        assert caplog.records[1].message == (
            f"'local_path' not specified, setting to '{self.zip_file_path}'"
        )

    def test_download_file_with_local_path(self, tmp_path) -> None:
        local_path: FilePathType = "leontief.jpg"
        download_and_save_file(self.jpg_url, tmp_path / local_path)
        with open(tmp_path / local_path) as test_saved_file:
            assert test_saved_file.name.endswith(str(local_path))

    def test_download_file_no_local_path(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)  # Enforce location to fit tmp_path
        download_and_save_file(self.jpg_url)
        assert (
            Path(extract_file_name_from_url(self.jpg_url)).stat().st_size == 63388
        )  # Previous result: 63366

    def test_extract_file_name_from_url_query_path(self, caplog) -> None:
        correct_uri_path: str = "/peoplepopulationandcommunity/populationandmigration/populationprojections/datasets/tablea11principalprojectionuksummary/2018based/ukpppsummary18.xls"
        caplog.set_level(DEBUG)
        assert ONS_UK_POPULATION_META_DATA.path
        assert isinstance(ONS_UK_POPULATION_META_DATA.path, Path)
        assert isinstance(ONS_UK_POPULATION_META_DATA.url, str)
        assert ONS_UK_POPULATION_META_DATA.path.name == extract_file_name_from_url(
            ONS_UK_POPULATION_META_DATA.url
        )
        correct_logs: list[str] = [
            f"Querying and parsing potential file paths from url: {ONS_UK_POPULATION_META_DATA.url}",
            f"Returning sole file path found: {correct_uri_path}",
        ]
        assert caplog.messages == correct_logs

    def test_download_query_url_file_no_local_path(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)  # Enforce location to fit tmp_path
        assert isinstance(ONS_UK_POPULATION_META_DATA.url, str)
        download_and_save_file(ONS_UK_POPULATION_META_DATA.url)
        assert (
            Path(extract_file_name_from_url(ONS_UK_POPULATION_META_DATA.url))
            .stat()
            .st_size
            == 371200  # Previous result: 367616
        )

    def test_register_and_read_file(self, caplog) -> None:
        if ONS_UK_POPULATION_META_DATA.is_local:
            ONS_UK_POPULATION_META_DATA.delete_local()
        assert not ONS_UK_POPULATION_META_DATA.is_local
        ONS_UK_POPULATION_META_DATA.save_local()
        assert ONS_UK_POPULATION_META_DATA.is_local
        df: DataFrame = ONS_UK_POPULATION_META_DATA.read()
        assert type(ONS_UK_POPULATION_META_DATA.dates) == list
        assert ([str(d) for d in ONS_UK_POPULATION_META_DATA.dates] == df.columns).all()
        assert df["2018"]["All ages"][0] == 66435.55  # Previous result: 64553.909
        caplog_message: str = (
            f"`sheet_name` parameter not provided, "
            f"defaulting to excel sheet to load: "
            f"{ONS_UK_POPULATION_META_DATA.absolute_save_path}"
        )
        assert caplog.messages == [caplog_message]
        ONS_UK_POPULATION_META_DATA.delete_local()


@dataclass
class TestMetaExample(ModelDataSourcesHandler):
    path_field: FilePathType
    any_source_field: MetaFileOrDataFrameType = field(default_factory=DataFrame)
    data_frame_field: DataFrame = field(default_factory=DataFrame)
    meta_data_field: MetaFileOrDataFrameType = field(
        default_factory=lambda: ONS_UK_POPULATION_META_DATA
    )
    meta_data_field_post_proc: MetaData = field(
        default_factory=lambda: ONS_UK_POPULATION_PROJECTIONS_BY_REGION_FROM_2018
    )

    path_file_name: Path = Path("test-local-meta-example.csv")

    def __str__(self):
        return f"TestMetaExample with example path: {self.path_file_name}"


@pytest.fixture
def test_array() -> ndarray:
    return randint(9, size=(3, 3))


@pytest.fixture
def meta_source_handler(
    tmp_path, monkeypatch, test_array
):  # -> Generator[TestMetaExample, None, None]:
    monkeypatch.chdir(tmp_path)  # Enforce location to fit tmp_path
    savetxt(TestMetaExample.path_file_name, test_array)
    yield TestMetaExample(path_field=tmp_path / TestMetaExample.path_file_name)


def filter_to_all_ages(df, all_ages_label="All ages", years=Iterable[str]) -> DataFrame:
    return df.loc[all_ages_label, years]


EXAMPLE_PROJECTIONS_YEAR_FILTER: Final[list[str]] = ["2030", "2040"]

ONS_UK_POPULATION_PROJECTIONS_BY_REGION_FROM_2018: MetaData = deepcopy(
    ONS_UK_POPULATION_META_DATA
)
ONS_UK_POPULATION_PROJECTIONS_BY_REGION_FROM_2018.name = (
    "UK ONS Population projection by Local Authority"
)
ONS_UK_POPULATION_PROJECTIONS_BY_REGION_FROM_2018._post_read_func = filter_to_all_ages
ONS_UK_POPULATION_PROJECTIONS_BY_REGION_FROM_2018._post_read_kwargs = dict(
    years=EXAMPLE_PROJECTIONS_YEAR_FILTER,
    df="meta_data_field",
)


class TestMetaSourceManager:

    """Assess basic features of managing data sources through MetaFileOrDataFrameType inheritance."""

    META_DATA_FIELD_LOG: str = (
        "Processing meta_data_field data file for: "
        "TestMetaExample with example path: test-local-meta-example.csv"
    )
    PATH_FIELD_LOG: str = (
        "Processing path_field data file for: "
        "TestMetaExample with example path: test-local-meta-example.csv"
    )

    def test_inheritance_example_prior_to_set_all(self, meta_source_handler) -> None:
        """Test default instantiation in a tmp_folder."""
        # monkeypatch.chdir(tmp_path)  # Enforce location to fit tmp_path
        assert (
            meta_source_handler._default_data_source_parser
            == pandas_from_path_or_package
        )
        assert field_names(meta_source_handler._meta_data_fields) == (
            "any_source_field",
            "meta_data_field",
            "meta_data_field_post_proc",
        )
        assert field_names(meta_source_handler._meta_file_or_dataframe_fields) == (
            "any_source_field",
            "meta_data_field",
            "meta_data_field_post_proc",
        )
        assert field_names(
            meta_source_handler._meta_file_or_dataframe_fields_strict
        ) == (
            "any_source_field",
            "meta_data_field",
        )
        assert field_names(meta_source_handler._meta_data_fields) == (
            "any_source_field",
            "meta_data_field",
            "meta_data_field_post_proc",
        )
        assert field_names(meta_source_handler._meta_data_fields_strict) == (
            "meta_data_field_post_proc",
        )
        assert field_names(meta_source_handler._processed_meta_data_attrs) == ()
        assert (
            field_names(
                meta_source_handler._processed_meta_data_with_post_read_func_attrs
            )
            == ()
            # assert field_names(meta_source_handler._meta_data_attrs) == ()
            # assert field_names(meta_source_handler._meta_file_or_dataframe_attrs) == (
            #     "any_source_field",
            #     "meta_data_field",
        )
        assert field_names(filter_fields_by_type(meta_source_handler, DataFrame)) == (
            "data_frame_field",
        )
        assert field_names(
            filter_fields_by_type(meta_source_handler, FilePathType)
        ) == ("path_field",)

    def test_path_examples(self, meta_source_handler, caplog) -> None:
        """Test default instantiation in a tmp_folder."""
        caplog.set_level(DEBUG)
        meta_source_handler._set_all_path_data_fields()
        assert (
            Path(meta_source_handler._path_field_path.name)
            == meta_source_handler.path_file_name
        )
        assert (
            meta_source_handler.path_field.size == 2
        )  # Default pandas csv reader includes first line as column names
        assert len(caplog.messages) == 1
        assert caplog.messages == [self.PATH_FIELD_LOG]

    def test_set_all_with_remote_permission_error(
        self, meta_source_handler, caplog
    ) -> None:
        """Test raising AutoDownloadPermissionError."""
        caplog.set_level(DEBUG)
        with pytest.raises(AutoDownloadPermissionError):
            meta_source_handler._set_all_meta_file_or_data_fields()
        assert caplog.messages == [self.META_DATA_FIELD_LOG]

    @pytest.mark.remote_data
    def test_set_all_with_correct_auto_download_permission(
        self, meta_source_handler, caplog
    ) -> None:
        """Test processing with AutoDownload permission fix."""
        caplog.set_level(DEBUG)
        meta_source_handler.meta_data_field.auto_download = True
        meta_source_handler.meta_data_field_post_proc._apply_post_read_func = True
        meta_source_handler._extract_post_read_kwargs_if_strs = True
        meta_source_handler._set_all_meta_file_or_data_fields()
        assert repr(
            meta_source_handler._meta_data_field__meta_data
            == "MetaData(name='UK ONS Population Projection', region='UK', year=2018)"
        )
        assert repr(
            meta_source_handler._meta_data_field_post_proc__meta_data
            == "MetaData(name='UK ONS Population projection by Local Authority', region='UK', year=2018)"
        )
        assert (
            meta_source_handler._meta_data_field__path
            == ONS_UK_POPULATION_META_DATA.path
        )
        assert (
            meta_source_handler._meta_data_field__url == ONS_UK_POPULATION_META_DATA.url
        )
        assert meta_source_handler.meta_data_field.size == 4949
        # assert len(caplog.messages) == 11
        assert len(caplog.messages) == 23
        assert caplog.messages[0] == self.META_DATA_FIELD_LOG
        meta_source_handler._processed_meta_data_post_read_func_attr_names == ()
        assert (
            meta_source_handler.meta_data_field_post_proc.columns
            == EXAMPLE_PROJECTIONS_YEAR_FILTER
        ).all()
        assert (
            meta_source_handler._meta_data_field_post_proc__raw.columns
            == meta_source_handler.meta_data_field.columns
        ).all()
        # meta_source_handler._set_all_meta_file_or_data_fields()
        # assert (
        #     meta_source_handler._meta_data_field_path
        #     == ONS_UK_POPULATION_META_DATA.path
        # )
        # assert (
        #     meta_source_handler._meta_data_field_url == ONS_UK_POPULATION_META_DATA.url
        # )
        # assert meta_source_handler.meta_data_field.size == 4949
        # assert len(caplog.messages) == 7
        # assert caplog.messages[0] == self.META_DATA_FIELD_LOG
