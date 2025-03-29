import pytest
import pandas as pd
import xml.etree.ElementTree as ET


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
    return pd.DataFrame(
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
    return pd.DataFrame(
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
