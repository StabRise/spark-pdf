from sparkpdf import PdfDataSource


def test_pdf_datasource(spark_session):
    spark_session.dataSource.register(PdfDataSource)
    df = spark_session.read.format("pdf").load("text_path").select("path")
    assert df.count() == 10
