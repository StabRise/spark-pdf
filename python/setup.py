from setuptools import setup, find_packages

setup(
    name="spark-pdf-python",
    version="1.1",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.3.0",
        "PyPDF2>=3.0.0",
        "Pillow>=9.0.0",
        "pytesseract>=0.3.0",
    ],
    author="aviral-bhardwaj",
    author_email="ardb40@gmail.com",
    description="PDF DataSource for Apache Spark in Python",
    long_description=open("spark-pdf/README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/spark-pdf",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)