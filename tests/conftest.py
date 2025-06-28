import pytest
import polars as pl
import lxml.etree as ET
from datetime import datetime, timedelta


@pytest.fixture
def sample_url():
    return "http://example.com"


@pytest.fixture
def sample_html_content():
    return """
    <html>
        <body>
            <a href="https://contrataciondelsectorpublico.gob.es/3_202502.zip">Link 1</a>
            <a href="https://contrataciondelsectorpublico.gob.es/3_2020.zip">Link 2</a>
        </body>
    </html>
    """


@pytest.fixture
def sample_atom_content():
    return """
    <feed xmlns="http://www.w3.org/2005/Atom">
        <entry>
            <title>Example Entry</title>
        </entry>
    </feed>
    """


@pytest.fixture
def sample_entries():
    entry_xml = """
    <entry xmlns="http://www.w3.org/2005/Atom">
        <title>Example Entry</title>
        <link href="http://example.com"/>
        <updated>2023-01-01T00:00:00Z</updated>
    </entry>
    """
    return [ET.fromstring(entry_xml)]


@pytest.fixture
def sample_nested_dict():
    return {"a": {"b": {"c": 1}}, "d": 2}


@pytest.fixture
def sample_period():
    return "202101"


@pytest.fixture
def sample_source_data():
    return {"202101": "https://example.com/contratacion202101.zip"}


@pytest.fixture
def sample_df_with_duplicates():
    return pl.DataFrame(
        [
            {
                "id": "1",
                "link": "http://example.com",
                "title": "Example Entry",
                "updated": "2023-01-01T00:00:00Z",
            },
            {
                "id": "1",
                "link": "http://example.com",
                "title": "Example Entry",
                "updated": "2023-01-02T00:00:00Z",
            },
        ]
    )


@pytest.fixture
def sample_mappings():
    return {"title": "title", "link": "link", "updated": "updated"}


@pytest.fixture
def sample_folder():
    return "data/202101"


@pytest.fixture
def sample_parquet_path():
    return "data/parquet/202101.parquet"


@pytest.fixture
def mock_source_data():
    return {"2023": "url1", "2022": "url2", "202301": "url3", "202302": "url4"}


@pytest.fixture
def sample_codice():
    return """
    <SimpleCodeList>
        <Row>
        <Value columnref="code">
            <SimpleValue>OBJ</SimpleValue>
        </Value>
        <Value columnref="nombre">
            <SimpleValue>Cuantificables Autom&#225;ticamente</SimpleValue>
        </Value>
        <Value columnref="name">
            <SimpleValue>Automatically evaluated</SimpleValue>
        </Value>
        </Row>
        <Row>
        <Value columnref="code">
            <SimpleValue>SUBJ</SimpleValue>
        </Value>
        <Value columnref="nombre">
            <SimpleValue>Juicio de Valor</SimpleValue>
        </Value>
        <Value columnref="name">
            <SimpleValue>Not automatically evaluated</SimpleValue>
        </Value>
        </Row>
    </SimpleCodeList>
    """


@pytest.fixture
def sample_df_for_mapping():
    return pl.DataFrame(
        [
            {
                "original_title": "Example Title",
                "original_link": "http://example.com",
                "original_date": "2023-01-01",
                "extra_field": "should be removed",
            }
        ]
    )


@pytest.fixture
def sample_mapping_dict():
    return {"original_title": "title", "original_link": "link", "original_date": "date"}


@pytest.fixture
def sample_data_list():
    return [
        {
            "title": "Example Entry",
            "link": "http://example.com",
            "updated": "2023-01-01T00:00:00Z",
        }
    ]


@pytest.fixture
def sample_df():
    return pl.DataFrame(
        [
            {
                "title": "Example Entry",
                "link": "http://example.com",
                "updated": "2023-01-01T00:00:00Z",
            }
        ]
    )


# Open Tenders specific fixtures
@pytest.fixture
def sample_open_tenders_data():
    """Sample open tenders data for testing"""
    return [
        {
            "title": "Test Tender 1",
            "link": "http://example.com/tender1",
            "updated": "2023-01-01T00:00:00Z",
            "ID": "123",
            "StatusCode": "PUB",
            "ContractingParty": "Test Party 1",
            "City": "Madrid",
            "Country": "Spain",
            "ZipCode": "28001",
            "ProjectTypeCode": "Suministros",
            "ProjectSubTypeCode": "001",
            "CPVCode": "30000000",
            "CPVLotCode": "30000000",
            "EstimatedAmount": "100000",
            "TotalAmount": "100000",
            "TaxExclusiveAmount": "82645",
            "ProcessCode": "OBJ",
            "ProcessEndDate": "2023-02-01T00:00:00Z",
        },
        {
            "title": "Test Tender 2",
            "link": "http://example.com/tender2",
            "updated": "2023-01-02T00:00:00Z",
            "ID": "456",
            "StatusCode": "PUB",
            "ContractingParty": "Test Party 2",
            "City": "Barcelona",
            "Country": "Spain",
            "ZipCode": "08001",
            "ProjectTypeCode": "Servicios",
            "ProjectSubTypeCode": "002",
            "CPVCode": "72000000",
            "CPVLotCode": "72000000",
            "EstimatedAmount": "200000",
            "TotalAmount": "200000",
            "TaxExclusiveAmount": "165289",
            "ProcessCode": "SUBJ",
            "ProcessEndDate": "2023-02-02T00:00:00Z",
        },
    ]


@pytest.fixture
def sample_open_tenders_df(sample_open_tenders_data):
    """Sample open tenders DataFrame"""
    return pl.DataFrame(sample_open_tenders_data)


@pytest.fixture
def sample_mapping_tables():
    """Sample mapping tables for testing"""
    return {
        "TenderingProcessCode": pl.DataFrame(
            {
                "code": ["OBJ", "SUBJ"],
                "nombre": ["Automatically evaluated", "Not automatically evaluated"],
            }
        ),
        "ContractCode": pl.DataFrame(
            {
                "code": ["Suministros", "Servicios", "Obras", "Patrimonial"],
                "nombre": ["Goods", "Services", "Works", "Patrimonial"],
            }
        ),
        "CPV2008": pl.DataFrame(
            {
                "code": ["30000000", "72000000"],
                "nombre": ["Office and computing machinery", "IT services"],
            }
        ),
        "GoodsContractCode": pl.DataFrame(
            {"code": ["001", "002"], "nombre": ["Goods Type 1", "Goods Type 2"]}
        ),
        "ServiceContractCode": pl.DataFrame(
            {"code": ["001", "002"], "nombre": ["Service Type 1", "Service Type 2"]}
        ),
        "WorksContractCode": pl.DataFrame(
            {"code": ["001", "002"], "nombre": ["Works Type 1", "Works Type 2"]}
        ),
        "PatrimonialContractCode": pl.DataFrame(
            {
                "code": ["001", "002"],
                "nombre": ["Patrimonial Type 1", "Patrimonial Type 2"],
            }
        ),
    }


@pytest.fixture
def sample_atom_entries():
    """Sample ATOM entries for testing"""
    from unittest.mock import MagicMock

    entries = []
    for i in range(2):
        entry = MagicMock()
        entry.tag = "{http://www.w3.org/2005/Atom}entry"

        # Mock title
        title = MagicMock()
        title.tag = "{http://www.w3.org/2005/Atom}title"
        title.text = f"Test Tender {i+1}"

        # Mock link
        link = MagicMock()
        link.tag = "{http://www.w3.org/2005/Atom}link"
        link.get.return_value = f"http://example.com/tender{i+1}"

        # Mock updated
        updated = MagicMock()
        updated.tag = "{http://www.w3.org/2005/Atom}updated"
        updated.text = f"2023-01-0{i+1}T00:00:00Z"

        entry.__iter__ = MagicMock(return_value=iter([title, link, updated]))
        entries.append(entry)

    return entries


@pytest.fixture
def sample_namespaces():
    """Sample XML namespaces for testing"""
    return {
        "cac-place-ext": "http://example.com/contract",
        "cac": "http://example.com/common",
        "cbc": "http://example.com/basic",
    }


@pytest.fixture
def sample_atom_entry_with_all_fields():
    """Sample ATOM entry with all required fields including ContractFolderStatus"""
    from unittest.mock import MagicMock

    # Create mock entry with proper structure
    mock_entry = MagicMock()
    mock_entry.tag = "{http://www.w3.org/2005/Atom}entry"

    # Mock field elements
    mock_title = MagicMock()
    mock_title.tag = "{http://www.w3.org/2005/Atom}title"
    mock_title.text = "Test Tender"

    mock_link = MagicMock()
    mock_link.tag = "{http://www.w3.org/2005/Atom}link"
    mock_link.get.return_value = "http://example.com"

    mock_updated = MagicMock()
    mock_updated.tag = "{http://www.w3.org/2005/Atom}updated"
    mock_updated.text = "2023-01-01T00:00:00Z"

    # Add id and summary fields that will be popped
    mock_id = MagicMock()
    mock_id.tag = "{http://www.w3.org/2005/Atom}id"
    mock_id.text = "test-id"

    mock_summary = MagicMock()
    mock_summary.tag = "{http://www.w3.org/2005/Atom}summary"
    mock_summary.text = "test summary"

    # Add ContractFolderStatus field that will be popped
    mock_contract_folder_status = MagicMock()
    mock_contract_folder_status.tag = (
        "{http://www.w3.org/2005/Atom}ContractFolderStatus"
    )
    mock_contract_folder_status.text = "test status"

    mock_entry.__iter__ = MagicMock(
        return_value=iter(
            [
                mock_title,
                mock_link,
                mock_updated,
                mock_id,
                mock_summary,
                mock_contract_folder_status,
            ]
        )
    )

    # Mock details with PUB status
    mock_details = MagicMock()
    mock_entry.find.return_value = mock_details

    return mock_entry


@pytest.fixture
def sample_flattened_details_pub():
    """Sample flattened details with PUB status for testing"""
    return {
        "ContractFolderStatusCode": "PUB",
        "ContractFolderID": "123",
        "LocatedContractingParty.Party.PartyName.Name": "Test Party",
        "LocatedContractingParty.Party.PostalAddress.CityName": "Madrid",
        "ProcurementProject.TypeCode": "Suministros",
        "TenderingProcess.TenderSubmissionDeadlinePeriod.EndDate": (
            datetime.now().date() + timedelta(days=2)
        ).strftime("%Y-%m-%d"),
    }


@pytest.fixture
def sample_flattened_details_non_pub():
    """Sample flattened details with non-PUB status for testing"""
    return {"ContractFolderStatusCode": "CLOSED", "ContractFolderID": "123"}


@pytest.fixture
def sample_raw_data_for_mapping():
    """Sample raw data for code mapping tests"""
    return pl.DataFrame(
        {
            "ProcessCode": ["OBJ", "SUBJ"],
            "ProjectTypeCode": ["Suministros", "Servicios"],
            "CPVCode": ["30000000", "72000000"],
            "CPVLotCode": ["30000000", "72000000"],
            "ProjectSubTypeCode": ["001", "002"],
        }
    )


@pytest.fixture
def sample_raw_data_with_unknown_codes():
    """Sample raw data with unknown codes for testing"""
    return pl.DataFrame(
        {
            "ProcessCode": ["OBJ", "UNKNOWN"],
            "ProjectTypeCode": ["Suministros", "UnknownType"],
            "CPVCode": ["30000000", "99999999"],
            "CPVLotCode": ["30000000", "99999999"],
            "ProjectSubTypeCode": ["001", "999"],
        }
    )


@pytest.fixture
def sample_mapped_data_with_metadata():
    """Sample mapped data with metadata for JSON conversion tests"""
    return pl.DataFrame(
        {
            "ID": ["123", "456"],
            "title": ["Tender 1", "Tender 2"],
            "updated": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            "ProcessCode": ["Automatically evaluated", "Not automatically evaluated"],
            "ProjectTypeCode": ["Goods", "Services"],
        }
    )


@pytest.fixture
def sample_mapped_data_without_updated():
    """Sample mapped data without updated field for JSON conversion tests"""
    return pl.DataFrame(
        {
            "ID": ["123", "456"],
            "title": ["Tender 1", "Tender 2"],
            "ProcessCode": ["Automatically evaluated", "Not automatically evaluated"],
            "ProjectTypeCode": ["Goods", "Services"],
        }
    )


@pytest.fixture
def sample_mapped_data_with_updated():
    """Sample mapped data with updated field for JSON conversion tests"""
    return pl.DataFrame(
        {
            "ID": ["123", "456"],
            "title": ["Tender 1", "Tender 2"],
            "updated": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            "ProcessCode": ["Automatically evaluated", "Not automatically evaluated"],
            "ProjectTypeCode": ["Goods", "Services"],
            "EstimatedAmount": [50000, 100000],
        }
    )


@pytest.fixture
def sample_codice_url():
    """Sample codice URL for testing"""
    return "https://contrataciondelestado.es/codice/cl/"


@pytest.fixture
def sample_codice_html():
    """Sample HTML content for codice website"""
    return """
    <html>
        <body>
            <a href="2023/">2023</a>
            <a href="2024/">2024</a>
            <a href="latest/">latest</a>
            <a href="old/">old</a>
        </body>
    </html>
    """


@pytest.fixture
def sample_version_html():
    """Sample HTML content for codice version page"""
    return """
    <html>
        <body>
            <a href="ContractCode-2023.gc">ContractCode</a>
            <a href="CPV2008-2023.gc">CPV2008</a>
            <a href="TenderingProcessCode-2023.gc">TenderingProcessCode</a>
            <a href="other-file.txt">Other file</a>
        </body>
    </html>
    """


@pytest.fixture
def sample_codice_xml():
    """Sample codice XML content"""
    return """
    <SimpleCodeList>
        <Row>
            <Value columnref="code">
                <SimpleValue>OBJ</SimpleValue>
            </Value>
            <Value columnref="nombre">
                <SimpleValue>Automatically evaluated</SimpleValue>
            </Value>
            <Value columnref="name">
                <SimpleValue>Automatically evaluated</SimpleValue>
            </Value>
        </Row>
        <Row>
            <Value columnref="code">
                <SimpleValue>SUBJ</SimpleValue>
            </Value>
            <Value columnref="nombre">
                <SimpleValue>Not automatically evaluated</SimpleValue>
            </Value>
            <Value columnref="name">
                <SimpleValue>Not automatically evaluated</SimpleValue>
            </Value>
        </Row>
    </SimpleCodeList>
    """


@pytest.fixture
def sample_parquet_files_data():
    """Sample data for creating parquet files"""
    return pl.DataFrame(
        {
            "id": ["1", "2", "3"],
            "title": ["Test 1", "Test 2", "Test 3"],
            "link": [
                "http://example.com/1",
                "http://example.com/2",
                "http://example.com/3",
            ],
            "updated": [
                "2023-01-01T00:00:00Z",
                "2023-01-02T00:00:00Z",
                "2023-01-03T00:00:00Z",
            ],
        }
    )


@pytest.fixture
def sample_codice_data():
    """Sample codice data for testing"""
    return {
        "ContractCode": ("https://example.com/ContractCode-2023.gc", "2023"),
        "CPV2008": ("https://example.com/CPV2008-2023.gc", "2023"),
        "TenderingProcessCode": (
            "https://example.com/TenderingProcessCode-2023.gc",
            "2023",
        ),
    }
